from pydantic import BaseModel, Field


class RunRequest(BaseModel):
    mode: str = Field(default="incremental", pattern="^(incremental|full)$")


class LookbackUpdate(BaseModel):
    lookback_seconds: int = Field(ge=0, le=86400 * 30)


class SettingUpdate(BaseModel):
    value: str


class RpaQueueRequest(BaseModel):
    target: str = Field(default="canonical", pattern="^(canonical|intake)$")


class RpaExecuteRequest(BaseModel):
    step_id: int | None = Field(default=None, ge=1)
    limit: int | None = Field(default=None, ge=1)
    mode: str = Field(default="incremental", pattern="^(incremental|full)$")


class RpaPromoteRequest(BaseModel):
    batch_id: int | None = Field(default=None, ge=1)
    intake_id: int | None = Field(default=None, ge=1)
    limit: int | None = Field(default=None, ge=1)
    mode: str = Field(default="incremental", pattern="^(incremental|full)$")
