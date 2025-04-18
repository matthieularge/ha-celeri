# Simple API Add-on for Home Assistant

This is a minimal Home Assistant add-on for Raspberry Pi that launches a FastAPI app using Gunicorn and responds with a simple REST message.

## Usage

After installing the add-on, access the API at:

```
GET /hello
Response: { "message": "Hello from my addon!" }
```

## Installation

Clone this repository into your Home Assistant `addons` folder:

```bash
git clone https://github.com/yourusername/simple_api_addon.git
```

Then go to **Supervisor > Add-on Store > ⋮ > Repositories**, add the path to your local add-ons folder, and install the add-on.

## License

MIT
