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

        stage('Terraform Init/Plan/Apply') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'AWS_Jen_IAM_Creds',
                                                 usernameVariable: 'AWS_ACCESS_KEY_ID',
                                                 passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                      export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
                      export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
                      terraform init
                      terraform plan
                      terraform apply -auto-approve
                    '''
                }
            }
        }

        stage('Glue Job Validation') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'AWS_Jen_IAM_Creds',
                                                 usernameVariable: 'AWS_ACCESS_KEY_ID',
                                                 passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                      export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
                      export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
                      aws glue get-job --job-name my-glue-job --query "Job.Name" --output text
                    '''
                }
            }
        }
        
        stage('Audit Logging') {
            steps {
                echo 'Audit trail stage placeholder'
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