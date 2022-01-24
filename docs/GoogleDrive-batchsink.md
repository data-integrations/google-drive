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

**Directory Identifier:** Identifier of the destination folder. This comes after “folders/” in the URL. For example, if 
the URL was “https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ”, then the 
Directory Identifier would be “1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g”.

### Authentication

**Authentication Type:** Type of authentication used to access Google API. Make sure Google Drive Api is enabled.
OAuth2 and Service account types are available.

#### OAuth2 Properties

OAuth2 client credentials can be generated on Google Cloud
[Credentials Page](https://console.cloud.google.com/apis/credentials)

**Client ID:** OAuth2 client id used to identify the application.

**Client Secret:** OAuth2 client secret used to access the authorization server.

**Refresh Token:** OAuth2 refresh token to acquire new access tokens.

For more details on OAuth2, see [Google Drive Api Documentation](https://developers.google.com/drive/api/v3/about-auth)

#### Service Account Properties

**Account File Path:** Path on the local file system of the service account key used for authorization. 
Can be set to 'auto-detect' for getting service account from system variable.
The file/system variable must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

* **File Path**: Path on the local file system of the service account key used for
  authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
  When running on other clusters, the file must be present on every node in the cluster.
  
  When set to 'auto-detect', the cluster needs to be configured with the scope required by the Google Drive API.
  Otherwise, the preview as well as pipeline run will fail with insufficient permission error. The required scope
  is "https://www.googleapis.com/auth/drive".

* **JSON**: Contents of the service account JSON file.

Make sure that the Google Drive Folder is shared with the service account email used. 
To write files to a Private Google Drive, grant the `Editor` role to the specified service account. 
To write files to a Shared Google Drive Folder, grant the `Contributor` role to the specified service account.