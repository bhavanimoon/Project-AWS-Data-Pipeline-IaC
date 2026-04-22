pipeline {
    agent any

    environment {
        AWS_DEFAULT_REGION = 'ap-south-2'   // Hyderabad region
    }

    stages {
        stage('Checkout') {
            steps {
                git(
                    url: 'https://github.com/bhavanimoon/Project-AWS-Data-Pipeline-IaC',
                    branch: 'main',
                    credentialsId: 'github-iac-aws-project-token' // GitHub PAT credential ID
                )
            }
        }

        stage('Terraform Init') {
            steps {
                sh 'terraform init'
            }
        }

        stage('Terraform Plan') {
            steps {
                sh 'terraform plan'
            }
        }

        stage('Terraform Apply') {
            steps {
                sh 'terraform apply -auto-approve'
            }
        }
    }

    post {
        success {
            echo 'Terraform applied successfully. Infrastructure is up to date.'
        }
        failure {
            echo 'Terraform apply failed. Check logs for details.'
        }
        always {
            archiveArtifacts artifacts: '**/terraform.tfstate', fingerprint: true
        }
    }
}