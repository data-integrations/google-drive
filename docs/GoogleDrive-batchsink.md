# Google Drive Batch Sink


Description
-----------
Sink plugin saves files from the pipeline to Google Drive directory via Google Drive API.

Properties
----------

### Basic

**File Body Field:** Name of the schema field (should be BYTES type) which will be used as body of file.
The minimal input schema should contain only this field.

**File Name Field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set files have randomly generated 16-symbols names.

**File Mime Field:** Name of the schema field (should be STRING type) which will be used as MIME type of file. 
All MIME types are supported except [Google Drive types](https://developers.google.com/drive/api/v3/mime-types).
Is optional. In the case it is not set Google API will try to recognize file's MIME type automatically.

**Directory Identifier:** Identifier of the destination folder.

This comes after `folders/` in the URL. For example, if the URL is
```
https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ
```
Then the Directory Identifier would be `1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g`.

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

Make sure that the Google Drive Folder is shared with the service account email. 
To write files to a Private Google Drive, grant the `Editor` role to the
specified service account email. To write files to a Shared Google Drive Folder, grant the `Contributor` role to the
specified service account.

* **Service Account File Path**: Path on the local file system of the service account key used for
  authorization.

  Can be set to 'auto-detect' when running on a Dataproc cluster which needs to be 
  created with the following scopes: 
  * https://www.googleapis.com/auth/drive

  When running on other clusters, the file must be present on every node in the cluster.

* **Service Account JSON**: Contents of the service account JSON file. Service Account JSON can be generated on Google Cloud
  [Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts) 
  

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
   scope=https%3A//www.googleapis.com/auth/drive&
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
6. Now, you will have your `refresh_token`, which is the last OAuth 2.0 property that the Google Drive Batch Sink needs
   to authorize with the Google Drive API.