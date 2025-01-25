from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import psycopg2
from passlib.context import CryptContext
import os
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Database connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# FastAPI Router
router = APIRouter()

# Request Model
class LoginRequest(BaseModel):
    anjni_id: str
    password: str
    activation_code: Optional[str] = None

# Login API
@router.post("/login")
def login_user(credentials: LoginRequest):
    cursor.execute("SELECT password_hash, activation_code, is_activated, activation_expiry FROM users WHERE anjni_id = %s", (credentials.anjni_id,))
    user = cursor.fetchone()

    if not user:
        raise HTTPException(status_code=400, detail="Invalid ANJNI ID")

    password_hash, stored_activation_code, is_activated, activation_expiry = user

    if not pwd_context.verify(credentials.password, password_hash):
        raise HTTPException(status_code=400, detail="Incorrect password")

    # If user is not activated, check activation code and expiry
    if not is_activated:
        if credentials.activation_code != stored_activation_code:
            raise HTTPException(status_code=400, detail="Invalid activation code")

        if datetime.utcnow() > activation_expiry:
            raise HTTPException(status_code=400, detail="Activation code expired")

        # Mark user as activated
        cursor.execute("UPDATE users SET is_activated = TRUE WHERE anjni_id = %s", (credentials.anjni_id,))
        conn.commit()
    else:
        # If already activated, activation code should not be required
        if credentials.activation_code:
            raise HTTPException(status_code=400, detail="Activation code is not required after first login")

    return {"message": "Login successful"}
