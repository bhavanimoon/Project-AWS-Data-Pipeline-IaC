# Production-Inspired Cloud Data Pipeline

Modern data platforms require more than just data processing—they require automated infrastructure, reliable deployments, scalable workflows, and operational visibility.

This repository demonstrates an end-to-end cloud-native data pipeline built using **Docker, Jenkins, Terraform, Python, and AWS**. The solution provisions infrastructure through **Infrastructure as Code (IaC)**, automates deployments using **CI/CD**, and processes CSV datasets through an event-driven serverless architecture.

Designed as a production-inspired implementation, the project emphasizes **automation, data quality, maintainability, and operational reliability**.

---

## 🎯 Project Intent

Having led enterprise cloud migration and data platform initiatives as a **Technical Project Manager**, I wanted to build a solution that reflects how modern cloud data pipelines are designed and delivered.

Rather than recreating a tutorial, the focus was on implementing practical validation, transformation, orchestration, and deployment patterns commonly found in production environments. This project challenged me to take an idea from architecture and design through implementation, deployment, testing, and operational monitoring.

---

## 🏗️ Solution Overview

### Development & Deployment

- Docker-based development environment
- GitHub source control
- Jenkins CI/CD with GitHub Webhooks
- Terraform Infrastructure as Code (IaC)
- Remote Terraform State (Amazon S3)

### AWS Data Pipeline

- Amazon EventBridge
- AWS Step Functions
- AWS Lambda Validation
- AWS Glue ETL Processing
- Amazon S3 Storage
- Amazon CloudWatch Monitoring

---

## 📁 Repository Structure

```text
Project-AWS-Data-Pipeline-IaC/

├── Artifacts/
├── Docker/
├── Policy/
├── Scripts/
├── Terraform/
├── Jenkinsfile
├── LICENSE
└── README.md
```

---

## ✨ Project Highlights

- **Dockerized Development Environment** for consistent local development.
- **Infrastructure as Code** using Terraform for automated resource provisioning.
- **Jenkins CI/CD Pipeline** integrated with GitHub Webhooks.
- **Event-Driven Serverless Architecture** using managed AWS services.
- **Multi-stage CSV Validation & ETL Processing** with AWS Lambda and AWS Glue.
- **Modular Repository Structure** designed for maintainability and future enhancements.
- **Centralized Monitoring & Logging** through Amazon CloudWatch.

---

## 👨‍💻 What This Project Demonstrates

This project reflects my ability to **design, build, and deliver cloud-native solutions** by combining infrastructure automation, workflow orchestration, data engineering, and project delivery into a single implementation.

It represents the way I approach technical projects—balancing engineering fundamentals with practical delivery considerations to build solutions that are reliable, maintainable, and production-inspired.