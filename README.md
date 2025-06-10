## Lambdas Local Development
### Build local environment
```bash
cd lambdas
sam build
```

### Invoke function locally
```bash
sam local invoke <function_name> -e ./<function_dir>/.event.json
```