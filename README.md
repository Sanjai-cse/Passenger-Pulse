## Passenger Pulse: Harnessing Data Analytics for Exceptional Customer Experience in Aviation

Passenger Pulse is a comprehensive data analytics platform designed to improve aviation passenger experience through insightful data processing and visualization.

## Features

- Real-time collection and processing of passenger and operational data.
  
- Interactive and dynamic dashboards created with Power BI.
  
- Storage of both unstructured (Couchbase Server) and structured data (PostgreSQL).
  
- Automated workflows and data pipeline scheduling with Apache Airflow.
  
- Containerized deployment using Docker for consistency and scalability.
  
- Version control and collaborative development using Git and GitHub.
  
- Data analysis and manipulation tools including Python and MS Excel.

## Architecture

- **Data Sources:** Gathering raw data from flight, booking, and passenger systems.
  
- **Storage Layer:** Unstructured data handled by Couchbase Server; structured relational data stored in PostgreSQL.
  
- **Processing Layer:** Python scripts handle cleaning, transforming, and analyzing data; orchestrated by Apache Airflow.
  
- **Visualization:** Power BI dashboards present actionable insights to stakeholders.
  
- **Containerization and Deployment:** Docker ensures environment parity across development and production.
  
- **Development Environment:** PyCharm IDE is used for efficient Python development and debugging.
  
- **Database Administration:** pgAdmin manages and administers PostgreSQL databases.

## Technology Stack

- **Programming Language:** Python (developed within PyCharm IDE)
  
- **Workflow Management:** Apache Airflow to schedule and monitor data pipelines
  
- **Databases:** Couchbase Server (NoSQL) and PostgreSQL (managed via pgAdmin)
  
- **Visualization:** Power BI
  
- **Containerization:** Docker
  
- **Version Control:** Git and GitHub
  
- **Data Analysis:** MS Excel

## Setup and Installation

1. Clone the repository:
   ```
   git clone https://github.com/Sanjai-cse/passenger-pulse.git
   cd passenger-pulse
   ```

2. Set up Python environment in PyCharm:

   - Create and activate a virtual environment
   - Install dependencies:
     ```
     pip install -r requirements.txt
     ```

3. Install and configure Couchbase Server and PostgreSQL:

   - Use pgAdmin for database setup and management.

4. Install and configure Apache Airflow:
   
   - Set up airflow dags, environment variables, and scheduler.

5. Ensure Docker is installed:
   
   - Build and run project containers via:
     ```
     docker-compose up --build -d
     ```

## Build and Run

1. Build Docker images:
   ```
   docker-compose build
   ```

2. Start Docker containers:
   ```
   docker-compose up -d
   ```

3. Run data processing workflows managed by Apache Airflow:
   - Start Airflow scheduler and webserver
   - Monitor DAG runs via Airflow UI

4. Run Python scripts for data analysis:
   ```
   python data_processing.py
   ```

5. Access Power BI dashboards locally or via published URLs.

***
