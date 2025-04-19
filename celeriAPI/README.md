# Celeri API Add-on for Home Assistant

This is a minimal Home Assistant add-on for Raspberry Pi that launches a FastAPI app using Gunicorn and responds with a simple REST message.

## Usage

After installing the add-on, access the API at:

```
GET /hello
Response: { "message": "Hello from my addon!" }
```
