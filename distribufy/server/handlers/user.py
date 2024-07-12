class User:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def to_dict(self) -> dict:
        return {"username": self.username, "password": self.password}

    @classmethod
    def from_dict(cls, data: dict) -> 'User':
        user = cls(data['username'], data['password'])
        user.password = data['password']
        return user
