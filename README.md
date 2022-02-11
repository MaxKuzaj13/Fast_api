# Fast_api
First use Fast api test


### To run it on terminal:
    1. Use command " uvicorn --port 8000 --host 127.0.0.1 main:app --reload"
    The command uvicorn main:app refers to:
        main: the file main.py (the Python "module").
        app: the object created inside of main.py with the line app = FastAPI().
        --reload: make the server restart after code changes. Only do this for development.

### to check internal documentation 
    Interactive API docs¶

    Now go to http://127.0.0.1:8000/docs.
    You will see the automatic interactive API documentation (provided by Swagger UI):


    
