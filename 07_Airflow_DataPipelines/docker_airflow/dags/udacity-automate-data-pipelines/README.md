# Data Pipeline (Airflow) Exercises


---

This folder and its contents come straight from Udacity. I intend to study these contents both for use
in the AWS data engineering course from Udacity and also as foundations for my own Airflow work.
-Kevin, Sep. 9, 2023

---

This repository can be used either in the Udacity workspace or in your own Airflow environment.

In either case, the `udacity/common` directory should be copied along with the `.airflowignore` files into the `/dag` directory of your airflow environment.

The `custom_operators` directory needs to be manually copied into the airflow `/plugins` directory of your aiflow environment.

The `set_connections.sh` script should be copied to the main `/home/workspace` directory in the workspace and updated with your own AWS and Redshit credentials. This will ensure that when you leave the workspace, later it will put those connections in place in Airflow.