
<h1>Apache Airflow Data Processing Workflows (Dockerized)</h1>

<p>
This project demonstrates building and orchestrating data processing workflows using
<strong>Apache Airflow</strong> and <strong>Docker</strong>.
It includes five production-style DAGs showcasing ingestion, transformation, export,
conditional branching, and notification/error-handling patterns.
</p>

<h2>Project Overview</h2>
<p>
The system runs entirely in Docker and uses <strong>PostgreSQL</strong> as both:
</p>
<ul>
    <li>Airflow metadata database</li>
    <li>Analytical data store</li>
</ul>
<p>
Final analytics-ready outputs are written in <strong>Parquet</strong> format for performance and portability.
</p>

<h2>Technology Stack</h2>
<div>
    <span class="badge">Apache Airflow 2.8.0</span>
    <span class="badge">Docker</span>
    <span class="badge">Docker Compose</span>
    <span class="badge">PostgreSQL</span>
    <span class="badge">Python</span>
    <span class="badge">Pandas</span>
    <span class="badge">PyArrow</span>
    <span class="badge">Pytest</span>
</div>

<h2>Project Structure</h2>
<pre>
project-root/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
├── tests/
│   ├── test_dag1.py
│   ├── test_dag2.py
│   └── test_utils.py
├── data/
│   └── input.csv
├── output/
│   └── .gitkeep
└── plugins/
    └── .gitkeep
</pre>

<h2>Prerequisites</h2>
<ul>
    <li>Docker Desktop (running)</li>
    <li>Docker Compose</li>
    <li>Git</li>
</ul>

<h2>Setup Instructions</h2>

<h3>1. Clone the Repository</h3>
<pre>
git clone &lt;your-repo-url&gt;
cd airflow-data-workflows
</pre>

<h3>2. Start Airflow</h3>
<pre>
docker-compose up -d
</pre>

<h3>3. Access Airflow UI</h3>
<p>
Open your browser and navigate to:
</p>
<pre>
http://localhost:8080
</pre>
<p>
<strong>Default credentials:</strong>
</p>
<ul>
    <li>Username: airflow</li>
    <li>Password: airflow</li>
</ul>

<h2>DAG Descriptions</h2>

<h3>DAG 1: CSV to PostgreSQL Ingestion</h3>
<ul>
    <li>Reads employee data from <code>data/input.csv</code></li>
    <li>Creates and truncates the target table</li>
    <li>Loads CSV data into PostgreSQL</li>
    <li>Ensures idempotency on repeated runs</li>
</ul>

<h3>DAG 2: Data Transformation Pipeline</h3>
<ul>
    <li>Reads raw employee data</li>
    <li>Applies transformations:
        <ul>
            <li>Full name + city concatenation</li>
            <li>Age group classification</li>
            <li>Salary category classification</li>
            <li>Year extraction from join date</li>
        </ul>
    </li>
    <li>Loads transformed data into a new table</li>
</ul>

<h3>DAG 3: PostgreSQL to Parquet Export</h3>
<ul>
    <li>Verifies source table existence</li>
    <li>Exports data to Parquet format</li>
    <li>Uses PyArrow with Snappy compression</li>
    <li>Validates schema and row count</li>
</ul>

<h3>DAG 4: Conditional Workflow</h3>
<ul>
    <li>Implements branching via <code>BranchPythonOperator</code></li>
    <li>Executes paths based on day of the week</li>
    <li>Ensures a common end task always runs</li>
</ul>

<h3>DAG 5: Notification Workflow</h3>
<ul>
    <li>Simulates a risky operation</li>
    <li>Triggers success or failure notifications</li>
    <li>Executes cleanup logic regardless of outcome</li>
    <li>Uses Airflow trigger rules correctly</li>
</ul>

<h2>Running Unit Tests</h2>
<p>
Tests validate DAG structure and logic without requiring a running database.
</p>
<pre>
docker exec -it airflow-webserver python -m pytest /opt/airflow/tests -v
</pre>

<h2>Input Data Format</h2>
<p>
The input CSV must contain at least <strong>100 rows</strong> with the following schema:
</p>
<pre>
id,name,age,city,salary,join_date
1,John Doe,28,New York,55000.50,2022-03-15
2,Jane Smith,35,San Francisco,75000.00,2021-07-22
</pre>

<h2>Troubleshooting</h2>
<pre>
docker-compose logs
</pre>

<p>To reset everything:</p>
<pre>
docker-compose down -v
docker-compose up -d
</pre>

<div class="warning">
    Ensure ports <strong>8080</strong> (Airflow) and <strong>5432</strong> (PostgreSQL) are not already in use.
</div>

<h2>Notes</h2>
<div class="note">
<ul>
    <li>Notification tasks simulate alerts only (no real emails)</li>
    <li>Unit tests focus on DAG integrity, not execution</li>
    <li>Credentials can be externalized using environment variables</li>
</ul>
</div>

<h2>Expected Outcomes</h2>
<ul>
    <li>Fully functional local Airflow environment</li>
    <li>Five operational DAGs visible in the UI</li>
    <li>PostgreSQL populated via CSV ingestion</li>
    <li>Parquet files generated in the output directory</li>
    <li>All unit tests passing</li>
</ul>

</body>
</html>
