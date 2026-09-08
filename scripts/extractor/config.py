import os
from dataclasses import dataclass, field


class ConfigError(RuntimeError):
    """Raised when required environment variables are missing or invalid."""
    pass


@dataclass(frozen=True)
class Config:
    readymode_url: str
    readymode_user: str
    readymode_password: str = field(repr=False)
    gcp_project: str
    raw_dataset: str = "dialer_dw_raw"

    @classmethod
    def from_env(cls) -> "Config":
        env_mappings = {
            "READYMODE_URL": "readymode_url",
            "READYMODE_USER": "readymode_user",
            "READYMODE_PASSWORD": "readymode_password",
            "GCP_PROJECT_ID": "gcp_project",
        }

        missing = []
        parsed = {}

        for env_var, attr in env_mappings.items():
            val = os.getenv(env_var)
            if not val:
                missing.append(env_var)
            else:
                parsed[attr] = val.rstrip("/") if attr == "readymode_url" else val

        if missing:
            missing_str = ", ".join(missing)
            raise ConfigError(
                f"Missing required environment variable(s): {missing_str}.\n"
                f"Please set these variables in your environment or .env file before running the extractor."
            )

        raw_dataset = os.getenv("BQ_RAW_DATASET", "dialer_dw_raw")
        parsed["raw_dataset"] = raw_dataset

        return cls(**parsed)
