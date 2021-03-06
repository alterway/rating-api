# **rating-api**

This flask API provides an interface to use the rated data coming out of the `rating-operator`.
It is packaged to run as a pod in a `kubernetes` cluster, and requires the `rating` stack to work.
Running it locally for debug purposes is also possible, using `run.sh`.

## Usage

Only the endpoints targeting the external use will be described in this document.
The `processing-operator` uses the `rating-api` to communicate with `presto` and `postgresql` internally.

## Grafana

Most of the endpoints below can be accessed through Grafana.
To enable it, the points below are required:

- Grafana needs to be version >= 7.0
- `simpod-json-datasource` module should be installed in your grafana instance

Once installed, you simply have to configure the datasource:

- Add a new `JSON` datasource
- If you have a default `rating-operator` deployment, use `rating-api.rating.svc.cluster.local` for address
- Whitelist the `session` cookie
- Enable `With credentials`


****

### Endpoints

**Every GET endpoints listed below can receive url arguments**

- `?start=&end=`
- start and end format is `%Y-%m-%d %H:%M:%S`

**If no arguments are provided, default values will be used.**

#### Healthcheck

`/alive`

- The alive endpoint is used to know if the service is running.

#### Rating

The available endpoints are sorted by category

#### Namespaces

**GET `/namespaces`**

- Get the list of namespaces.

**GET `/namespaces/rating`**

- Get the rating per namespaces, per hour and per metric.

**GET `/namespaces/total_rating`**

- Get the total rating per namespaces.

**GET `/namespaces/<namespace>/total_rating`**

- Get the total rating of a given namespace.

**GET `/namespaces/<namespace>/rating`**

- Get the price for a given namespace, per hour and per metric.

**GET `/namespaces/<namespace>/pods`**

- Get a list of pods for the given namespace.

**GET `/namespaces/<namespace>/nodes`**

- Get a list of nodes for the given namespace.

**GET `/namespaces/<namespace>/nodes/pods`**

- Get the number of pods hosted on nodes for a given namespace.

#### Metrics

**GET `/metrics`**

- Get the list of metrics.

**GET `/metrics/<metric>/rating`**

- Get the rating for a given metric, per hour.

**GET `/metrics/<metric>/total_rating`**

- Get the total rating for a given metric.

**GET `/reports/<report>/metric`**

- Get the metric for a given report.

**GET `/metrics/<metric>/report`**

- Get the report for a given metric.

**GET `/metrics/<metric>/last_rated`**

- Get the date of the last rating for a given metric.

#### Pods

**GET `/pods`**

- Get the list of pods.

**GET `/pods/<pod>/lifetime`**

- Get the start and last update time of a pod.

**GET `/pods/rating`**

- Get the rating for a given pod per hour.

**GET `/pods/total_rating`**

- Get the total rating for a given pod.

**GET `/pods/<pod>/rating`**

- Get the rating for a given pod, per hour and per metric.

**GET `/pods/<pod>/total_rating`**

- Get the total rating for a given pod.

**GET `/pods/<pod>/namespace`**

- Get the namespace for a given pod.

**GET `/pods/<pod>/node`**

- Get the node for a given pod.

#### Nodes

**GET `/nodes`**

- Get the list of nodes.

**GET `/nodes/rating`**

- Get the rating per nodes, per hour and per metric.

**GET `/nodes/total_rating`**

- Get the total rating per nodes.

**GET `/nodes/<node>/rating`**

- Get the price for a given node, per hour and per metric.

**GET `/nodes/<node>/namespaces`**

- Get the namespaces related to a given node.

**GET `/nodes/<node>/pods`**

- Get the pods hosted on a given node.

**GET `/nodes/<node>/total_rating`**

- Get the total rating of a given node.

**GET `/nodes/<node>/namespaces/rating`**

- Get the rating of the namespaces for a given node, per hour and per metric.

**GET `/nodes/<node>/namespaces/<namespace>/rating`**

- Get the rating of the given namespace for a given node, per hour and per metric.

**GET `/nodes/<node>/namespaces/<namespace>/total_rating`**

- Get the total rating of the given namespace for a given node.

#### Configurations

**GET `/rating/configs`**

- Get the list of all configurations as object 

**GET `/rating/configs/list`**

- Get the list of all configurations as timestamps

**GET `/rating/configs/<timestamp>`**

- Get the configuration for a given timestamp

**POST `/rating/configs/add`**

- Create a new configuration according to the POST request received
- Expect a `body` object containing `rules`, `metrics` and `timestamp` as json

**POST `/rating/configs/update`**

- Update a configuration according to the POST request received
- Expect a `body` object containing `rules`, `metrics` and `timestamp` as json

**POST `/rating/configs/delete`**

- Delete the configuration according to the POST request received
- Expect a `body` object containing `timestamp`


#### Multi-tenancy

**`/signup`**

- Render the `signup` template, send the form to `/signup_user`

**POST `/signup_user`**

- Create the user in the database and provides namespace
- Expects **tenant** and **password**
- **quantity** can also be provided, default to 1

**POST `/login`**

- Render the `login` template, send the form to `/login_user`

**POST `/login_user`**

- Log the user in the application, returns a openned session
- Expects **tenant** and **password**

**`/logout`**

- Close your actual session

**GET `/current`**

- Returns the tenant holding the session

**GET `/tenant`**

- Expect **tenant**
- Returns the tenant and its namespaces

**GET `/tenants`**

- Returns the list of tenants

**POST `/tenants/link`**

- Link namespaces to a given tenant
- Expect **tenant** and **namespaces** (list of identifiers)

**POST `/tenants/unlink`**

- Unlink a namespace from its tenants
- Expect **namespace**

**POST `/tenants/delete`**

- Delete a given tenant and all its associated namespaces
- Expect **tenant**