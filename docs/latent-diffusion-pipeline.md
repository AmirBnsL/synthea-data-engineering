# Latent Diffusion Pipeline — Synthetic Retinal OCT/CFP (Phase 1.7)

## Architecture

```
                          ┌─────────────────────────────────────┐
                          │       OFFLINE — MODEL TRAINING       │
                          │  (GPU, Docker container)             │
                          │                                     │
                          │  Public Datasets:                   │
                          │  ┌──────┐ ┌──────┐ ┌──────┐        │
                          │  │MESSI-│ │Eye-  │ │OCTID │        │
                          │  │DOR   │ │PACS  │ │      │        │
                          │  └──┬───┘ └──┬───┘ └──┬───┘        │
                          │     │        │        │              │
                          │     └────────┴────────┘              │
                          │     MONAI preprocessing pipeline     │
                          │              │                       │
                          │     ┌────────▼────────┐              │
                          │     │  Fine-tune LDM   │             │
                          │     │  (LoRA adapters) │             │
                          │     │  via Diffusers   │             │
                          │     └────────┬────────┘              │
                          │              │ MLflow tracks         │
                          │              │ FID, params, epochs   │
                          └──────────────┼───────────────────────┘
                                         │ LoRA weights .safetensors
                                         ▼
┌───────────────────────────────────────────────────────────────────────┐
│                     RUNTIME — IMAGE GENERATION                         │
│                                                                       │
│  Synthea output ──► Spark/Iceberg ──► raw_landing.patients            │
│   (FHIR)                        ──► raw_landing.conditions            │
│                                        (E11.3xxx diabetic retinopathy) │
│                                             │                          │
│                                             ▼                          │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │           Latent Diffusion Service (separate GPU container)      │  │
│  │                                                                   │  │
│  │  1. Read DR patients from Iceberg (via Spark or direct read)     │  │
│  │  2. Map diagnosis_code → generation prompt                       │  │
│  │     ┌────────────────┬──────────────────────────────────────┐   │  │
│  │     │ E11.3299 (mild)│ "OCT B-scan, mild NPDR, few MA"     │   │  │
│  │     │ E11.3319 (mod) │ "CFP, moderate NPDR, IRF, CW"       │   │  │
│  │     │ E11.3391 (PDR) │ "CFP, proliferative DR, NVD"        │   │  │
│  │     └────────────────┴──────────────────────────────────────┘   │  │
│  │  3. DiffusionPipeline(prompt, num_inference_steps=50).output    │  │
│  │     └─► 512×512 PNG retinal image                                │  │
│  │  4. Upload to MinIO: s3://raw-landing/images/{patient_id}/...   │  │
│  │  5. Write metadata row to Iceberg: raw_landing.retinal_images   │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
```

## Motivation

Synthea generates **structured EHR data** — demographics, diagnoses, labs, medications — but no medical images. Diabetic retinopathy (DR) is a vision-threatening complication of diabetes that is diagnosed and graded via retinal imaging (OCT B-scans, color fundus photography). A multimodal synthetic dataset that pairs DR patient records with realistic retinal images is required for:

- Training computer-aided diagnosis models without real patient data
- Privacy-preserved ophthalmology AI research
- Evaluating cross-modal retrieval (ICD-10 codes → images → clinical notes)

## Tool Stack

| Tool | Purpose |
|------|---------|
| **Hugging Face Diffusers** | LDM architecture, schedulers, training utilities |
| **PyTorch 2.x** | Training/inference, GPU acceleration |
| **MONAI** | Medical image loading, augmentation, transforms |
| **LoRA (PEFT)** | Parameter-efficient fine-tuning (~15MB adapter vs 5GB full model) |
| **MLflow** | Experiment tracking, FID logging, model registry |
| **MinIO** (`mc` / `boto3`) | Store generated images as binary objects |
| **Apache Iceberg** | `retinal_images` table mapping image → patient → MinIO path |
| **PySpark** | Read DR patients from Iceberg, write image metadata back |
| **Docker (GPU)** | `nvidia-docker` runtime for training + generation service |

## Training Pipeline

### Data Acquisition

| Dataset | Images | Modality | Labels |
|---------|--------|----------|--------|
| MESSIDOR | 1,200 | CFP | DR grade 0–3 |
| EyePACS (Kaggle) | 88,000+ | CFP | DR grade 0–4 |
| OCTID | 500+ | OCT B-scan | Normal / AMD / DME |
| DRIMDB | 250 | CFP | Normal / abnormal |

### Preprocessing (MONAI)

```
Dataset ──► LoadImage ──► Resize(512) ──► ScaleIntensity ──► ToTensor
```

- All images resized to 512×512 (LDM native resolution)
- Labels encoded as conditioning text: e.g., `"OCT B-scan, moderate NPDR with macular edema"`
- Dataset split: 80% train, 10% val, 10% test

### Model Architecture

Base: **Stable Diffusion 2.1-base** (512px, 860M params)

```python
from diffusers import StableDiffusionPipeline, UNet2DConditionModel
from peft import LoraConfig, get_peft_model

unet = UNet2DConditionModel.from_pretrained("stabilityai/stable-diffusion-2-1-base", subfolder="unet")
lora_config = LoraConfig(r=16, lora_alpha=32, target_modules=["to_q", "to_k", "to_v", "to_out.0"])
unet = get_peft_model(unet, lora_config)
```

Only the **LoRA adapters** on U-Net attention layers are trained. VAE and text encoder are frozen.

### Training Configuration

```yaml
# config.yaml
model:
  base: "stabilityai/stable-diffusion-2-1-base"
  resolution: 512
  lora_rank: 16

training:
  batch_size: 8
  learning_rate: 1e-4
  optimizer: "AdamW"
  num_epochs: 100
  mixed_precision: "fp16"

data:
  datasets: ["messidor", "eyepacs", "octid", "drimdb"]
  train_split: 0.8
  augmentation: ["random_flip", "brightness_contrast"]
```

Loss: standard LDM denoising objective

$$L = \mathbb{E}_{x, c, \epsilon, t} \left[ \|\epsilon - \epsilon_\theta(z_t, t, \tau_\theta(c))\|_2^2 \right]$$

### MLflow Tracking

```python
with mlflow.start_run() as run:
    mlflow.log_params(config.training)
    for epoch in range(100):
        train_loss = train_epoch()
        fid = compute_fid(val_dataloader, pipeline)
        mlflow.log_metrics({"train_loss": train_loss, "fid": fid}, step=epoch)
    mlflow.transformers.log_model(
        transformers_model={"pipeline": pipeline},
        artifact_path="ldm_lora_adapter",
        registered_model_name="retinal_diffusion"
    )
```

### Model Registry

| Version | R² | FID | Notes |
|---------|-----|-----|-------|
| 1 | 100 | 35.2 | Baseline (pure general LDM) |
| 2 | 100 | 22.8 | + MESSIDOR LoRA |
| 3 | 100 | 15.1 | + MESSIDOR + EyePACS LoRA |

## Generation Pipeline

### Step 1: Read DR Patients from Iceberg

```python
spark.table("raw.raw_landing.conditions") \
    .filter("condition_text LIKE '%diabetic retinopathy%'") \
    .join(spark.table("raw.raw_landing.patients"), "patient_id") \
    .select("patient_id", "encounter_id", "condition_text") \
    .collect()
```

### Step 2: Diagnosis Code → Prompt Mapping

```python
PROMPT_MAP = {
    "Background diabetic retinopathy": "Retinal photograph, mild NPDR, few microaneurysms, hard exudates",
    "Moderate non-proliferative diabetic retinopathy": "OCT B-scan, moderate NPDR, intraretinal fluid, cotton wool spots",
    "Severe non-proliferative diabetic retinopathy": "Retinal photograph, severe NPDR, venous beading, IRMA",
    "Proliferative diabetic retinopathy with new vessels": "CFP, proliferative DR, neovascularization of the disc",
}
```

### Step 3: Generate Images

```python
from diffusers import StableDiffusionPipeline
import boto3

pipeline = StableDiffusionPipeline.from_pretrained("stabilityai/stable-diffusion-2-1-base")
pipeline.unet.load_adapter("models/retinal_lora_v3.safetensors")
pipeline.to("cuda")

for row in dr_patients:
    prompt = PROMPT_MAP.get(row.condition_text, "Retinal fundus photograph")
    image = pipeline(prompt, num_inference_steps=50, guidance_scale=7.5).images[0]

    # Upload to MinIO
    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="admin", aws_secret_access_key="password")
    path = f"images/{row.patient_id}/{uuid4()}.png"
    image.save(f"/tmp/{path}")
    s3.upload_file(f"/tmp/{path}", "raw-landing", path)
```

### Step 4: Write Metadata to Iceberg

```
raw_landing.retinal_images
┌──────────┬────────────┬──────────────┬──────────────┬────────┬──────────────────────────┐
│image_id  │patient_id  │encounter_id  │diagnosis_code│modality│minio_path                │
├──────────┼────────────┼──────────────┼──────────────┼────────┼──────────────────────────┤
│img_4f2a  │e2e7e6b..   │enc_123       │E11.3319      │OCT     │s3://raw-landing/images/.. │
│img_8bc1  │e2e7e6b..   │enc_123       │E11.3299      │CFP     │s3://raw-landing/images/.. │
└──────────┴────────────┴──────────────┴──────────────┴────────┴──────────────────────────┘
PARTITIONED BY (days(generated_at))
```

```sql
CREATE TABLE IF NOT EXISTS raw.raw_landing.retinal_images (
    image_id       STRING,
    patient_id     STRING,
    encounter_id   STRING,
    diagnosis_code STRING,
    condition_text STRING,
    modality       STRING,
    minio_path     STRING,
    generated_at   TIMESTAMP
) USING iceberg PARTITIONED BY (days(generated_at));
```

## Integration with Existing Pipeline

```
      ┌────────────────────────────────────────────────────┐
      │              Existing Airflow DAG                    │
      │  generate_fhir → upload_fhir → fhir_to_iceberg      │
      └────────────────────────┬───────────────────────────┘
                               │
                               ▼
      ┌────────────────────────────────────────────────────┐
      │              New: generate_retinal_images           │
      │  (DockerOperator, runs on GPU container)            │
      │                                                     │
      │  1. Query raw_landing.conditions for DR codes       │
      │  2. Load LoRA adapter from MLflow Model Registry    │
      │  3. Batch generate images (GPU)                     │
      │  4. Upload to MinIO                                 │
      │  5. Append to raw_landing.retinal_images            │
      └────────────────────────────────────────────────────┘
```

### Proposed new files

```
synthea/
├── diffusion/
│   ├── Dockerfile.gpu        # CUDA + Diffusers + MONAI + MLflow
│   ├── config.yaml           # Training and generation config
│   ├── train.py              # LoRA fine-tuning on retinal datasets
│   ├── generate.py           # Batch generation from Iceberg DR patients
│   ├── prompts.py            # Diagnosis code → natural language prompt
│   └── schemas.py            # Iceberg table writer
└── dags/
    └── synthea_to_minio.py   # ← +generate_retinal_images task
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Model architecture | LoRA fine-tune on SD 2.1-base | Medical imagery shares low-level visual features with general imagery — full fine-tuning is wasted compute |
| Image size | 512×512 | LDM native resolution; larger = OOM on consumer GPUs |
| Condition method | Text prompt | Simpler than ControlNet for initial release; can upgrade to ControlNet (edge-conditioned) later |
| Storage | MinIO object store + Iceberg metadata | Follows existing project pattern; images remain queryable via Spark SQL |
| Generation trigger | Separate Airflow task | Avoids GPU dependency in Spark container; clean separation of concerns |
