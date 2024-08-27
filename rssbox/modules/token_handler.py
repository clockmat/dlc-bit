from sonicbit.handlers import TokenHandler as BaseTokenHandler
from pymongo.collection import Collection
from sonicbit.types import AuthResponse


class TokenHandler(BaseTokenHandler):
    def __init__(self, accounts: Collection):
        self.accounts = accounts

    def read(self, email: str) -> str | None:
        account = self.accounts.find_one({"_id": email})
        if not account:
            return None
        
        return account.get("token")
    
    def write(self, email: str, auth: AuthResponse) -> None:
        self.accounts.update_one({"_id": email}, {"$set": {"token": auth.token}}, upsert=True)