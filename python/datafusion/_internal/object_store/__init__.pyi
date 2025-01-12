from typing import List, Optional, Tuple


class LocalFileSystem:
    def __init__(self, prefix: Optional[str] = None) -> None: ...

class MicrosoftAzure:
    def __init__(
        self,
        container_name: str,
        account: Optional[str] = None,
        access_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        sas_query_pairs: Optional[List[Tuple[str, str]]] = None,
        use_emulator: Optional[bool] = None,
        allow_http: Optional[bool] = None
        ) -> None: ...

class GoogleCloud:
    def __init__(
        self,
        bucket_name: str,
        service_account_path: Optional[str] = None,
        ) -> None: ...
    
class AmazonS3:
    def __init__(
        self,
        bucket_name: str,
        region: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        allow_http: bool = False,
        imdsv1_fallback: bool = False,
        ) -> None: ...
    
class Http:
    def __init__(self, url: str) -> None: ...

