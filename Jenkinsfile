pipeline {
    agent any

    environment {
        PATH = "/usr/bin:${env.PATH}"
        AWS_DEFAULT_REGION = 'ap-south-2'
    }

    tools {
        git 'Git-Default'
    }

    stages {
    //     stage('Checkout') {
    //         steps {
    //             git(
    //                 url: 'https://github.com/bhavanimoon/Project-AWS-Data-Pipeline-IaC',
    //                 branch: 'main',
    //                 credentialsId: 'github-iac-aws-project-token'   // GitHub PAT credential ID
    //             )
    //         }
    //     }

        stage('Verify Git Tool') {
            steps {
                sh 'which git && git --version'
            }
        }

        stage('Check PATH') {
            steps {
                sh 'echo $PATH'
            }
        }    

        stage('Checkout') {
            steps {
                checkout([$class: 'GitSCM',
                    branches: [[name: '*/main']],
                    userRemoteConfigs: [[
                        url: 'https://github.com/bhavanimoon/Project-AWS-Data-Pipeline-IaC.git',
                        credentialsId: 'github-iac-aws-project-token'
                    ]],
                    extensions: []
                ])
            }
        }

        stage('Terraform Init') {
            steps {
                sh 'cd Terraform && terraform init -upgrade -migrate-state'
            }
        }

        stage('Terraform Plan') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'AWS_Jen_IAM_Creds',
                                                 usernameVariable: 'AWS_ACCESS_KEY_ID',
                                                 passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                      cd Terraform
                      export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
                      export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
                      terraform plan -out=tfplan
                    '''
                }
                archiveArtifacts artifacts: 'Terraform/tfplan', fingerprint: true
            }
        }

        stage('Manual Approval') {
            steps {
                input message: 'Approve Terraform Apply?', ok: 'Apply'
            }
        }

        stage('Terraform Apply') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'AWS_Jen_IAM_Creds',
                                                 usernameVariable: 'AWS_ACCESS_KEY_ID',
                                                 passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                      cd Terraform
                      export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
                      export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
                      terraform apply -auto-approve tfplan
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
                      aws glue get-job --job-name glue-data-processor-job --region ap-south-2
                    '''
                }
            }
        }

        stage('Audit Logging') {
            steps {
                sh '''
                  echo "Commit: ${env.GIT_COMMIT}, Build: ${env.BUILD_ID}" >> audit.log
                  aws s3 cp audit.log s3://my-audit-bucket/${env.BUILD_ID}/audit.log
                '''
            }
        }
    }

    post {
        success {
            echo 'Terraform applied successfully.'
        }
        failure {
            echo 'Terraform apply failed.'
        }
        always {
            archiveArtifacts artifacts: '**/terraform.tfstate', fingerprint: true
        }
    }
}