from fastapi import HTTPException
def f():
    try:
        pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
