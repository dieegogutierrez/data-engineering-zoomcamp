## GCP Overview

[Video](https://www.youtube.com/watch?v=18jIzE41fJ4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=2)


### Project infrastructure modules in GCP:
* Google Cloud Storage (GCS): Data Lake
* BigQuery: Data Warehouse

(Concepts explained in Week 2 - Data Ingestion)

### Initial Setup

For this course, we'll use a free version (upto EUR 300 credits). 

1. Create an account with your Google email ID 
2. Setup your first [project](https://console.cloud.google.com/) if you haven't already
    * eg. "DTC DE Course", and note down the "Project ID" (we'll use this later when deploying infra with TF)
3. Setup [service account & authentication](https://cloud.google.com/docs/authentication/getting-started) for this project
    * Grant `Viewer` role to begin with.
    * Download service-account-keys (.json) for auth.
4. Download [SDK](https://cloud.google.com/sdk/docs/quickstart) for local setup
5. Set environment variable to point to your downloaded GCP keys:
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   
   # Refresh token/session, and verify authentication
   gcloud auth application-default login
   ```
   
### Setup for Access
 
1. [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
   * Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
   * Click the *Edit principal* icon for your service account.
   * Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
   
2. Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
   
3. Please ensure `GOOGLE_APPLICATION_CREDENTIALS` env-var is set.
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   ```
 
### Terraform Workshop to create GCP Infra
Continue [here](./terraform): `week_1_basics_n_setup/1_terraform_gcp/terraform`

### Personal notes

#### Initial setup
- Create a project 
- Go to IAM & Admin>service accounts and set up a service account
- Grant service account access to project: Cloud storage>Storage Admin, BigQuery>BigQuery Admin
- After created if needed to add a different service, go to IAM and edit the service account. Compute engine>compute admin
- Adding permisions to use the service account, still in IAM/service accounts on actions, choose Manage Keys>ADD KEY>Create new key>JSON
- Google Terraform google provider>Use provider, copy and paste in main.tf
- Credentials can be inserted in main.tf or using:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
```
#### Refresh token/session, and verify authentication
```bash
gcloud auth application-default login
```
#### Create a bucket
- Cloud storage>Buckets
- Google>Terraform google storage bucket and paste it to main.tf

#### Create a BigQuery
- Google>terraform bigquery dataset, look for the required parameters and paste it to main.tf

### Extra notes

#### Generate ssh keys to connect local machine to cloud
https://cloud.google.com/compute/docs/connect/create-ssh-keys?_gl=1*1hgkamz*_ga*MTIxMzQ4MjE3MC4xNzA0NjY2Mzgz*_ga_WH2QY8WWF5*MTcwNTk0NzI4MC4xMy4xLjE3MDU5NDcyODIuMC4wLjA.&_ga=2.155409339.-1213482170.1704666383

#### Change KEY_FILENAME and USERNAME. gcp and diegogutierrez
```bash
ssh-keygen -t rsa -f ~/.ssh/KEY_FILENAME -C USERNAME -b 2048 
```
#### Input ssh public key to gcp
Compute Engine>Metadata>SSH KEYS
Copy and paste the ssh public key here, from terminal you can use "cat gcp.pub"

#### Create a virtual machine on gcp
- Name
- Region
- Instance (What type of machine is needed)
- Boot Disc (Ubuntu and Size 30gb)

#### Connecting local machine and cloud
- Get the external IP of virtual machine
- Type in terminal where private ssh key is in local machine, and username and external IP from cloud
```bash
ssh -i ~/.ssh/gcp diegogutierrez@34.118.165.44
```
#### Install Anaconda
```bash
wget https://repo.anaconda.com/archive/Anaconda3-2023.09-0-Linux-x86_64.sh
bash Anaconda...
```
#### Create a config file so it is possible to connect only typing ssh de-zoomcamp
- Go to .ssh folder on local machine
```bash
touch config
```
- Copy and paste
Host de-zoomcamp
    HostName 34.118.165.44
    User diegogutierrez
    IdentityFile ~/.ssh/gcp

#### Install docker
```bash
sudo apt-get install docker.io
sudo apt-get update
sudo apt-get install docker.io
```
#### Configuring visual code to interact with VM
- Install remote ssh extension
- Open remote window(down left corner)

#### Clone de-zoomcamp repo
```bash
git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
```
#### Run docker hello-world, it needs adm permission, adding permission so it is not necesssary to apply sudo everytime
- docker without sudo
https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md
```bash
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart
```
- Logout and login

#### Install docker compose
- Create a bin directory
https://github.com/docker/compose
- Under releases get the link for system linux 86/64
```bash
wget https://github.com/docker/compose/releases/download/v2.24.2/docker-compose-linux-x86_64 -O docker-compose
```
- Make the file executable
```bash
chmod +x docker-compose
```
#### Make docker compose visible from anywhere and not just from bin. Edit bashrc file
- Go to home directory
```bash
nano .bashrc
```
- Go to the end of file and write, so any executables in bin directory will be installed/executed. Save and close it.
```bash
export PATH="${HOME}/bin:${PATH}"
```
- To apply changes
```bash
source .bashrc
```
#### Install pgcli
- Go to home directory
```bash
pip install pgcli
```
- Accessing the table
```bash
pgcli -h localhost -U root -d ny_taxi
```
#### Install pgcli using conda
- If get any errors look for update conda or update pgcli directly
```bash
conda install -c conda-forge pgcli
pip install -U mycli
```
#### Access postgres, pgadmin and jupyter locally, forward ports in visual code
ctrl + ~
- Go to ports and include ports

#### Install terraform at bin directory
```bash
wget https://releases.hashicorp.com/terraform/1.7.0/terraform_1.7.0_linux_amd64.zip
```
#### Google Cloud SDK Authentication 
- On local envinronment
```bash
sftp de-zoomcamp
```
- Create directory .gc
```bash
mkdir .gc
```
- Upload JSON file to directory
```bash
put ny-rides.json
```
- Set `GOOGLE_APPLICATION_CREDENTIALS` to point to the file
```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/ny-rides.json
```
- Now authenticate: 
```bash
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
```
#### Connect to VM after stopping it
- Get the new external IP
- Go to .ssh folder
```bash
nano config
```
- Replace HostName
