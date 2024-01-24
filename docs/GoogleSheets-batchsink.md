# Google Sheets Batch Sink


Description
-----------
Writes spreadsheets to specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the destination folder.<br>

This comes after `folders/` in the URL. For example, if the URL is
```
https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ
```
Then the Directory Identifier would be `1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g`.

**Spreadsheet Name Field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set Google API will use the value of **Default Spreadsheet name** property.

**Default Spreadsheet Name:** Default spreadsheet file name. 
Is used if user doesn't specify schema field with spreadsheet name.

**Sheet Name Field:** Name of the schema field (should be STRING type) which will be used as sheet title. 
Is optional. In the case it is not set Google API will use the value of **Default sheet name** property.

**Default Sheet Name:** Default sheet title. Is used when user doesn't specify schema field with sheet title.

**Write Schema As First Row:** Toggle that defines if the sink writes out the input schema as first row of an 
output sheet.

### Authentication

**Authentication Type:** Type of authentication used to access Google API.

OAuth2 and Service Account types are available.

Make sure that:
* `Google Drive API` and `Google Sheets API` is enabled in the `GCP Project`.
* `Google Drive Folder` is shared to the service account email used with the required permission.

#### OAuth2 Properties

OAuth2 client credentials can be generated on Google Cloud
[Credentials Page](https://console.cloud.google.com/apis/credentials)

**OAuth Method:** The method used to get OAuth access tokens. The oauth access token can be directly provided,
or a client id, client secret, and refresh token can be provided.

**Access Token:** Short lived access token for connect.

**Client ID:** OAuth2 client id used to identify the application.

**Client Secret:** OAuth2 client secret used to access the authorization server.

**Refresh Token:** OAuth2 refresh token to acquire new access tokens.

For more details on OAuth2, see [Google Drive Api Documentation](https://developers.google.com/drive/api/v3/about-auth)

#### Service Account Type

Make sure that the Google Drive Folder is shared with the specified service account email. 
To write files to a Private Google Drive, grant the `Editor` role to the specified service account.
To write files to a Shared Google Drive Folder, grant the `Contributor` role to the specified service account.

* **Service Account File Path**: Path on the local file system of the service account key used for
  authorization.

  Can be set to 'auto-detect' when running on a Dataproc cluster which needs to be
  created with the following scopes:
  * https://www.googleapis.com/auth/drive
  * https://www.googleapis.com/auth/spreadsheets

  When running on other clusters, the file must be present on every node in the cluster.

* **Service Account JSON**: Contents of the service account JSON file. Service Account JSON can be generated on Google Cloud
  [Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

### Buffering And Paralellization

**Threads Number:** Number of threads which send batched API requests. 
The greater value allows to process records quickly, but requires extended Google Sheets API quota.

**Maximal Buffer Size:** Maximal size in records of the batch API request. 
The greater value allows to reduce the number of API requests, but causes increase of their size.

**Records Queue Length:** Size of the queue used to receive records and for onwards grouping of them to 
batched API requests. With the greater value it is more likely that the sink will group received records in the 
batches of maximal size. Also greater value leads to more memory consumption.

**Maximum Flush Interval:** Number of seconds between the sink tries to get batched requests from the records queue 
and send them to threads for sending to Sheets API.

**Flush Execution Timeout:** Timeout for single thread to process the batched API request. 
Be careful, the number of retries and maximal retry time also should be taken into account.

### Advanced

**Minimal Page Extension Size:** Minimal size of sheet extension when default sheet size is exceeded.

**Merge Data Cells:** Toggle that defines if the sink merges data cells created as result of 
input arrays flattering.

**Skip spreadsheet/sheet name fields:** Toggle that defines if the sink skips spreadsheet/sheet name
fields during structure record transforming.

### Steps to Generate OAuth2 Credentials
1. Create credentials for the Client ID and Client Secret properties [here](https://console.cloud.google.com/apis/credentials).
2. On the Create OAuth client ID page, under Authorized redirect URIs, specify a URI of `http://localhost:8080`.
   This is just to generate the `refresh token`.
3. Click `Create`. The OAuth client is created. For more information, see this [doc](https://developers.google.com/adwords/api/docs/guides/authentication#webapp).
4. Copy the Client ID and Client Secret to the plugin properties.
5. To get the Refresh Token, follow these steps:
   1. Authenticate and authorize with the Google Auth server to get an authorization `code`.
   2. Use that authorization code with the Google Token server to get a `refresh token` that the plugin will use to get future access tokens.

   To get the authorization code, you can copy the URL below, change to use your `client_id`, and
   then open that URL in a browser window.
   ```
   https://accounts.google.com/o/oauth2/v2/auth?
   scope=https%3A//www.googleapis.com/auth/drive%20https://www.googleapis.com/auth/spreadsheets&
   access_type=offline&
   include_granted_scopes=true&
   response_type=code&                  
   state=state_parameter_passthrough_value&
   redirect_uri=http%3A//localhost:8080&
   client_id=199375159079-st8toco9pfu1qi5b45fkj59unc5th2v1.apps.googleusercontent.com
   ```
   This will prompt you to login, authorize this client for specified scopes,
   and then redirect you to `http://localhost:8080`. It will look like an error page,
   but notice that the URL of the error page redirected to include the `code`.
   In a normal web application, that is how the authorization code is returned to the requesting web application.
   
   For example, URL of the page will be something like
   ```
   http://localhost:8080/?state=state_parameter_passthrough_value&code=4/0AX4XfWi6PsiJiPO4MjltrcD6uoRgwci-HX16aL1-Ax-tgqYgC47NnjtCCKRoVzv46m8aJw&scope=https://www.googleapis.com/auth/drive
   ```
   Here, code=`4/0AX4XfWi6PsiJiPO4MjltrcD6uoRgwci-HX16aL1-Ax-tgqYgC47NnjtCCKRoVzv46m8aJw`.

   **NOTE**: If you see an error like this `Authorization Error — Error 400: admin_policy_enforced`,
   then the GCP User’s organization has a policy that restricts you from using Client IDs for third party products.
   In that case, they’ll need to get that restriction lifted, or use a different GCP user in a different org.
   
   With that authorization code, you can now call the Google Token server to get the `access token` and
   the `refresh token` in the response. Set the `code`, `client_id`, and `client_secret` in the curl command below and
   run it in a Cloud Shell terminal.
   ```
   curl -X POST -d "code=4/0AX4XfWjgRdrWXuNxqXOOtw_9THZlwomweFrzcoHMBbTFkrKLMvo8twSXdGT9JramIYq86w&client_id=199375159079-st8toco9pfu1qi5b45fkj59unc5th2v1.apps.googleusercontent.com&client_secret=q2zQ-vc3wG5iF5twSwBQkn68&redirect_uri=http%3A//localhost:8080&grant_type=authorization_code&access_type=offline" \
   https://oauth2.googleapis.com/token
   ```
6. Now, you will have your `refresh_token`, which is the last OAuth 2.0 property that the Google Sheets Batch Sink needs
   to authorize with the Google Drive and Google Sheets API.