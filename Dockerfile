FROM python:3.11-alpine 


# Set the working directory
WORKDIR /espresso
COPY . .

RUN pip install -r requirements.txt
ENV INSTANCE_MODE WEB

# Add a script or command to run by default (optional)
CMD ["python3", "app.py"]