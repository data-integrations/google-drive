# Google Drive Batch Sink


Description
-----------
Sink plugin saves files from the pipeline to Google Drive directory via Google Drive API.

Properties
----------

**File body field** Name of the schema field (should be BYTES type) which will be used as body of file.
The minimal input schema should contain only this field.

**File name field** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set files have randomly generated 16-symbols names.

**Directory identifier** Identifier of the destination folder.

**Client ID** OAuth2 client id.

**Client secret** OAuth2 client secret.

**Refresh token** OAuth2 refresh token.

**Access token** OAuth2 access token.