Documentation

How to Run the Dockerized Application
1.Prerequisites
    Docker Desktop installed and running on your machine.
    A Python application (e.g., app.py) in your project directory.
    Optional: A requirements.txt file if your app has dependencies.

2.Project Structure
    User-Requesters
    --- app.py              # Python application
    --- Dockerfile          # Docker configuration file
    --- requirements.txt    # Optional: List of Python dependencies

3.Build the Docker Image
    Open a terminal in the User-Requesters/ directory.
    Run the following command:  docker build -t my-python-app .
    This creates an image named my-python-app.

4.Run the Container
To run with the default environment variable:   docker run my-python-app
Output: Application started! TEST_VARIABLE: Default Test Value

To inject a custom $TEST_VARIABLE:  docker run -e TEST_VARIABLE="Your Custom Value" my-python-app
Output: Application started! TEST_VARIABLE: Your Custom Value

5.Troubleshooting
If the container fails to start, check the logs with:   docker run my-python-app
Ensure app.py is executable and has no syntax errors.
If using requirements.txt, list dependencies like requests==2.28.1 (one per line).