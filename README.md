# Ecompal-product-manager-service

This repository contains the code for the core ecompal product import API.

Endpoints Include: 

- User: Create and authentication users
- Job - Creating and updating jobs (jobs are tasks created for importing products into shopify)
- upload - Accepts an excel or csv file and returns the details of the file (ie. products and their details). 
- run - start a job for creating products on shoopify.

The application uses several AWS resources, including Lambda functions and an API Gateway API, and SNS. These resources are defined in the `template.yaml` file in this project. You can update the template to add AWS resources through the same deployment process that updates your application code.
