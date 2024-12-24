# Confidential Workload Demo - Mule Accounts
This project provides a demo of a confidential workload to share mule accounts.

---

## Features

- **Confidential Data Processing**: Handles secure workloads while maintaining privacy.
- **Data Quality Checks**: Triggers data holders' quality checks.
- **Collaborative Analysis**: Enables secure analysis across data owners.

---

## Project Structure

```
├── .github 
├── ── workflows
├── ── ──release_docker_image.yaml  #  sample code to build and publish the docker image via github actions 
├── data             # datasets examples used in the demo and loaded by the confidential workload 
├── outputs          # example of outputs
├── test             # unit tests
├── .env.example     # env example to run locally
├── Dockerfile       # Docker configuration for containerized deployment
├── index.py         # Entry point for orchestrating events
├── LICENSE.txt      # License information (MIT License)
├── process.py       # Core processing logic for confidential workloads
├── README.md.txt    # Readme file
├── requirements.txt # List of required Python packages
```

---

## Deployment process
What happens when such a repo is pushed on a github repository ?

The dockerfile is being used to build a docker image that is then published on the github container registry.
You can then refer to this docker image via its registry address to make the datavillage platform download and run it in a confidential environment

---

## License

This project is licensed under the MIT License. See `LICENSE.txt` for details.