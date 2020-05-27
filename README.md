# Machinery Python SDK

This is a subset Python implementation of a worker for `RichardKnop/machinery`.

## Limitations

- Broker is strictly only for Redis
- BackendResult is strictly only for MongoDB

## Development Environment

Use `venv` with python 3.

```shell
# ---- Setup
$ sudo apt install python3 python3-venv
$ python3 -m venv ~/.venv/simcel
# ---- Activate venv
$ source ~/.venv/simcel/bin/activate
(simcel) $ pip3 install -r requirements.txt
# ---- Running
(simcel) $ python3 main.py
```

## Usage

```python
from pymachinery import Value, Machinery

def add_worker(a: Value, b: Value):
    return (Value("int64", a.value() + b.value()))

def multiply_worker(a: Value, b: Value):
    return (Value("int64", a.value() * b.value()))

def worker(name: Value, delayms: Value):
    return (
        Value("string", f"hello {name.value()}"),
        Value("int64", delayms),
    )

machinery = Machinery(
    broker_uri="redis://:helloworld@localhost:6379",
    backend_uri="mongodb://mongo:moonbucks@localhost:27017/?authSource=admin",
    queue="machinery_tasks",
)
machinery.register_worker("add", add_worker)
machinery.register_worker("multiply", multiply_worker)
machinery.register_worker("hello", worker)
machinery.start()
```

### Creating a Worker

Workers have these restrictions:

- Inputs should be of type pymachinery.Value
- Outputs should be of type pymachinery.Value, or a tuple of pymachinery.Value
