# Google Drive Batch Source


Description
-----------
Reads a fileset from specified Google Drive directory via Google Drive API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the source folder.

This comes after `folders/` in the URL. For example, if the URL is
```
https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ
```
Then the Directory Identifier would be `1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g`.

**File Identifier:** Identifier of the file.

This comes after `file/d/ or document/d/ or spreadsheets/d/` in the URL. For example, if the URL is
```
https://docs.google.com/file/d/17W3vOhBwe0i24OdVNsbz8rAMClzUitKeAbumTqWFrkows
```
Then the File Identifier would be `17W3vOhBwe0i24OdVNsbz8rAMClzUitKeAbumTqWFrkows`.  
Either Directory Identifier or File Identifier should have a value. Filters will not work 
while providing File Identifier.

**File Metadata Properties:** Properties that represent metadata of files. 
They will be a part of output structured record. Descriptions for properties can be view at 
[Drive API file reference](https://developers.google.com/drive/api/v3/reference/files).

### Filtering

**Filter:** Filter that can be applied to the files in the selected directory. 
Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).

**Modification Date Range:** Filter that narrows set of files by modified date range. 
User can select either among predefined or custom entered ranges. 
For _Custom_ selection the dates range can be specified via **Start Date** and **End Date**. 

**Start Date:** Start date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification Rate Range** field. 
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**End Date:** End date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification Date Range** field.
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**File Types To Pull:** Types of files which should be pulled from a specified directory. 
The following values are supported: binary (all non-Google Drive formats), Google Documents, Google Spreadsheets, 
Google Drawings, Google Presentations and Google Apps Scripts. 
For Google Drive formats user should specify exporting format in **Exporting** section.

### Authentication

**Authentication Type:** Type of authentication used to access Google API. 

OAuth2 and Service Account types are available.

Make sure that:
* `Google Drive API` is enabled in the `GCP Project`.
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
`Viewer` role must be granted to the specified service account to read files from the Google Drive Folder.

* **Service Account File Path**: Path on the local file system of the service account key used for
  authorization.

  Can be set to 'auto-detect' when running on a Dataproc cluster which needs to be
  created with the following scopes:
  * https://www.googleapis.com/auth/drive.readonly

  When running on other clusters, the file must be present on every node in the cluster.

* **Service Account JSON**: Contents of the service account JSON file. Service Account JSON can be generated on Google Cloud
  [Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

### Advanced

**Maximum Partition Size:** Maximum body size for each structured record specified in bytes. 
Default 0 value means unlimited. Is not applicable for files in Google formats.

**Body Output Format** Output format for body of file. "Bytes" and "String" values are available.

### Exporting

**Google Documents Export Format:** MIME type which is used for Google Documents when converted to structured records.

**Google Spreadsheets Export Format:** MIME type which is used for Google Spreadsheets when converted to structured records.

**Google Drawings Export Format:** MIME type which is used for Google Drawings when converted to structured records.

**Google Presentations Export Format:** MIME type which is used for Google Presentations when converted to structured records.

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
   scope=https%3A//www.googleapis.com/auth/drive.readonly&
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
6. Now, you will have your `refresh_token`, which is the last OAuth 2.0 property that the Google Drive Batch Source needs 
   to authorize with the Google Drive API.