from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel
import pyodbc

app = FastAPI()
# Basic route to check if the server is running
@app.get("/health")
async def health_check():
    return {"status": "ok"}
# Root route to return a simple message
@app.get("/")
async def root():
    return {"message": "Hello World"}

# Database connection setup
server = 'DESKTOP-1M0C5L8\SQLEXPRESS01'
database = 'fast_test'
username = 'sa' 
password = 'your_password'  # Replace with your actual password
# Connection string for SQL Server
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
# Ensure the database and table exist before running the application

# Pydantic model
class User(BaseModel):
    empId: int
    name: str
    address: str
    phone: int

class UserPatch(BaseModel):
    name: str = None
    address: str = None
    phone: int = None
   

def get_db_connection():
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print("Connection Error: ", e)
        return None


# Get All Users
@app.get("/users/")
def read_all_user():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute("SELECT EmpId, Name, Address, Phone FROM test")
    rows = cursor.fetchall() 
    conn.close()
    return [dict(zip([column[0] for column in cursor.description], row)) for row in rows]

# Get Users by ID
@app.get("/users/{id}")
def read_user(id: int = Path(..., description="The ID of the user to retrieve")):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute("SELECT EmpId, Name, Address, Phone FROM test WHERE EmpId = ?", id)
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(zip([column[0] for column in cursor.description], row))
    else:
        raise HTTPException(status_code=404, detail="User not found")

# Create a New User
@app.post("/users/")
def create_user(user: User):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO test (EmpID, Name, Address, Phone) VALUES (?, ?, ?, ?)",
        (user.empId, user.name, user.address, user.phone)
    )
    conn.commit()
    conn.close()
    return {"message": "User created successfully"}

# Update an Existing User Based on EmpId
@app.put("/users/{id}")
def update_user(id: int, user: User):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE test SET Name = ?, Address = ?, Phone = ? WHERE EmpId = ? ",
        (user.name, user.address, user.phone, id)
    )
    conn.commit()
    conn.close()
    return {"message": "User updated successfully"}

# Delete an Existing User on EmpId
@app.delete("/users/{id}")
def delete_user(id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM test WHERE EmpId = ?", id)
    conn.commit()
    conn.close()
    return {"message": "User deleted successfully"}

@app.patch("/users/{id}")
def patch_user(id: int, user_patch: UserPatch):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    cursor = conn.cursor()
    
    # Build the update query dynamically
    update_fields = []
    values = []

    if user_patch.name is not None:
        update_fields.append("Name = ?")
        values.append(user_patch.name)
    if user_patch.address is not None:
        update_fields.append("Address = ?")
        values.append(user_patch.address)
    if user_patch.phone is not None:
        update_fields.append("Phone = ?")
        values.append(user_patch.phone)

    if not update_fields:
        conn.close()
        raise HTTPException(status_code=400, detail="No fields provided for update")

    sql = f"UPDATE test SET {', '.join(update_fields)} WHERE EmpId = ?"
    values.append(id)

    cursor.execute(sql, values)
    conn.commit()
    conn.close()

    return {"message": "User updated successfully"}


# Define a port
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
