# Google Drive Batch Sink


Description
-----------
Sink plugin saves files from the pipeline to Google Drive directory via Google Drive API.

Properties
----------

### Basic

**File body field:** Name of the schema field (should be BYTES type) which will be used as body of file.
The minimal input schema should contain only this field.

**File name field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set files have randomly generated 16-symbols names.

**File mime field:** Name of the schema field (should be STRING type) which will be used as MIME type of file. 
All MIME types are supported except [Google Drive types](https://developers.google.com/drive/api/v3/mime-types).
Is optional. In the case it is not set Google API will try to recognize file's MIME type automatically.

**Directory identifier:** Identifier of the destination folder.

### Authentication

**Client ID:** OAuth2 client id.

**Client secret:** OAuth2 client secret.

**Refresh token:** OAuth2 refresh token.

**Access token:** OAuth2 access token.