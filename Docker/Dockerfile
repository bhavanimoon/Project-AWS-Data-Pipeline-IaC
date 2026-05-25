FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget unzip curl git python3 python3-pip \
    openjdk-21-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark for Glue job testing
RUN pip3 install pyspark==3.5.0

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip && ./aws/install

# Install Terraform
RUN curl -fsSL https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip -o terraform.zip \
    && unzip terraform.zip && mv terraform /usr/local/bin/ && rm terraform.zip

# Install Jenkins (LTS)
RUN wget https://get.jenkins.io/war-stable/latest/jenkins.war -O /usr/share/jenkins.war

# Expose Jenkins port
EXPOSE 8080

# Start Jenkins
CMD ["java", "-jar", "/usr/share/jenkins.war"]