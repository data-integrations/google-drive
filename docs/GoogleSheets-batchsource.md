# Google Sheets Batch Source


Description
-----------
Reads spreadsheets from specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the source folder. This comes after “folders/” in the URL. For example, if the URL was 
“https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ”, 
then the Directory Identifier would be “1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g”.

### Filtering

**Filter:** Filter that can be applied to the files in the selected directory. 
Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).

**Modification Date Range:** Filter that narrows set of files by modified date range. 
User can select either among predefined or custom entered ranges. 
For _Custom_ selection the dates range can be specified via **Start date** and **End date**. 

**Start Date:** Start date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field. 
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**End Date:** End date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field.
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**Sheets To Pull:** Filter that specifies set of sheets to process. 
For 'numbers' or 'titles' selections user can populate specific values in 'Sheets identifiers' field.

**Sheets Identifiers:** Set of sheets' numbers/titles to process. 
Is shown only when 'titles' or 'numbers' are selected for 'Sheets to pull' field.

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
  When using 'auto-detect', the cluster needs to be configured with the scope required by the Google Drive Api,
  otherwise the preview as well as pipeline run will fail with insufficient permission error. Here, the required scope
  is "https://www.googleapis.com/auth/drive.readonly".

* **JSON**: Contents of the service account JSON file.

Make sure that the Google Drive Folder is shared to the service account email used. `Viewer` role must be granted to
the specified service account to read files from the Google Drive Folder.

### Metadata Extraction

**Extract Metadata:** Field to enable metadata extraction. Metadata extraction is useful when user wants to specify 
a header or a footer for a sheet. The rows in headers and footers are not available as data records. 
Instead, they are available in every record as a field called 'metadata', which is a record of the specified metadata.

**Metadata Field Name:** Name of the record with metadata content. 
It is needed to distinguish metadata record from possible column with the same name.

**First Header Row Index:** Row number of the first row to be treated as header.

**Last Header Row Index:** Row number of the last row to be treated as header.

**First Footer Row Index:** Row number of the first row to be treated as footer.

**Last Footer Row Index:** Row number of the last row to be treated as footer.

**Metadata Cells:** Set of the cells for key-value pairs to extract as metadata from the specified metadata sections.
Only shown if **Extract metadata** is set to true. The cell numbers should be within the header or footer.

### Advanced

**Text Formatting:** Output format for numeric sheet cells. 
In 'Formatted values' case the value will contain appropriate format of source cell e.g. '1.23$', '123%'." 
For 'Values only' only number value will be returned.

**Skip Empty Data:** Field that allows skipping of empty structure records.

**Add spreadsheet/sheet name fields:** Toggle that defines if the source extends output schema with 
spreadsheet and sheet names.

**Spreadsheet field name:** Schema field name for spreadsheet name.

**Sheet field name:** Schema field name for sheet name.

**Column Names Selection:** Source for column names. User can specify where from the plugin should get schema filed names.
Are available following values: _No column names_ - default sheet column names will be used ('A', 'B' etc.), 
_Treat first row as column names_ - the plugin uses first row for schema defining and field names,
 _Custom row as column names_ - as previous, but for custom row index.

**Custom Row Index For Column Names:** Number of the row to be treated as a header.
Only shown when the 'Column Names Selection' field is set to 'Custom row as column names' header.

**Last Data Column Index:** Last column plugin will read as data.

**Last Data Row Index:** Last row plugin will read as data.

**Read Buffer Size:** Number of rows the source reads with a single API request.