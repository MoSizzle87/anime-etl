# ── Imports ───────────────────────────────────────────────────────────────────
import os
from dataclasses import dataclass
from typing import Optional

# ── Constantes ────────────────────────────────────────────────────────────────
MAX_RETRIES: int = 3
PI: float = 3.14159
APP_NAME: str = "MyApp"


# ── Enum-like ─────────────────────────────────────────────────────────────────
class Status:
    ACTIVE = "active"
    INACTIVE = "inactive"


# ── Dataclass ─────────────────────────────────────────────────────────────────
@dataclass
class User:
    name: str
    age: int
    email: Optional[str] = None

    def greet(self) -> str:
        return f"Hello, {self.name}"

    @staticmethod
    def validate_age(age: int) -> bool:
        return age > 0

    @classmethod
    def from_dict(cls, data: dict) -> "User":
        return cls(name=data["name"], age=data["age"])


# ── Fonction standalone ───────────────────────────────────────────────────────
def process_users(users: list[User], max_count: int = 10) -> list[str]:
    results = []
    for user in users:
        if user.age >= 18:
            results.append(user.greet())
    return results[:max_count]


# ── Lambda & variable ─────────────────────────────────────────────────────────
is_adult = lambda age: age >= 18
multiplier = lambda x, factor=2: x * factor

# ── Utilisation ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    alice = User(name="Alice", age=30, email="alice@example.com")
    bob = User.from_dict({"name": "Bob", "age": 17})

    users = [alice, bob]
    greetings = process_users(users, max_count=5)

    for msg in greetings:
        print(msg)

    path = os.path.join("/tmp", APP_NAME)
    print(f"Status: {Status.ACTIVE}, Path: {path}, Adult: {is_adult(alice.age)}")
