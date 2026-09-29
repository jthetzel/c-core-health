# Claude plan
We have an application described in @../holmes-2/ . It consists of a python API in @../holmes/components/ and @../holmes/bases/ . The react typescript frontend is in @../holmes-2/frontend/ . The database migrations are in @../holmes-2/alembic/ . The database is available with this port forward:
```
kubectl port-forward services/pgstac 5433:5432 --namespace hasura --context c-core
```
The username is `username`. The password is `password`.

Workflow orchestration is handled by Prefect. We can access the Prefect Server with:
```
kubectl port-forward service/prefect-server 4200:4200 -n prefect --context c-core
```

We have many Prefect Flows cataloged in @../holmes-2/components/prefect/flows.py. The CI code for these flows and APIs are described in @../holmes-2/projects/ and @../holmes-2/deployments/ .

The main purpose of this application is to detect targets and AIS data in satellite scenes, mostly Sentinel 1 and RCM SAR data.

## The problem
Occasionally we have a data processing error in a Prefect Deployment Flow. It can be difficult to determine which `scene_id` the error effects (or perhaps multiple scene_ids). All the various Prefect Flows have unique random UUIDs and some run in parallel, but might rely on each other downstream, making it diffifult to know which Prefect Flows need to be re-run. Also, when a provessing workflow breaks, it can leave data behind in a broken state.

## The goal
We want to build Python FastAPI API that lists recent SAR scene_ids, a few summary statistics (e.g. number of targets detected, type of targets detected), and a button that re-runs the parent Prefect Flow deployment. Optionally, we want to be able to delete as data associated with that scene_id so we can re-run from a clean state in case of collisions with data from the failed flow run. Later, we will add a web dashboard for interacting with the API. The 

Please create a plan. You may think hard. Please follow data science engineering best practices.
