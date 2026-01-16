from pydantic_settings import BaseSettings
from urllib.parse import quote_plus 

class Settings(BaseSettings):
    env: str

    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str

    @property
    def db_dsn(self) -> str:
        password_encoded = quote_plus(self.db_password)
        return f"postgresql://{self.db_user}:{password_encoded}@{self.db_host}:{self.db_port}/{self.db_name}"

    class Config:
        env_file = "../.env"

settings = Settings()