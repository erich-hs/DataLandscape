## Lambdas Local Development
### Build local environment
The [sam build](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-build.html) evaluates the template.yaml and local requirements.txt files to build the environment for local testing.
```bash
cd lambdas
sam build
```

### Invoke function locally
While at the lambdas directory, run:
```bash
sam local invoke <function_name> -e ./<function_dir>/.event.json
```

### Test lambdas
Lambda tests should be run from the root directory for access to the shared requirements-dev.txt file, and usage of the Makefile.

To test all lambdas, run the following command from the root directory:
```bash
make test-lambdas
```