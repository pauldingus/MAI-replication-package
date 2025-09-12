var ee = require('@google/earthengine');
const {GoogleAuth} = require('google-auth-library');

initialize = function (onsuccess, auth, opt_project) {
  if(auth) {
    console.log('Resetting authentication is not supported in this flow.');
    console.log('Please re-run `gcloud auth application-default login` to authenticate.');
    return;
  }
  (async () => {
    try {
      const googleAuth = new GoogleAuth({
        scopes: [
          'https://www.googleapis.com/auth/earthengine',
          'https://www.googleapis.com/auth/devstorage.full_control'
        ]
      });
    const client = await googleAuth.getClient();
    const tokens = await client.getAccessToken();

    ee.apiclient.setAuthToken('', 'Bearer', tokens.token, 3600, [], undefined, false);

    let project = opt_project;
    if(typeof(project) === 'undefined') {
      if(typeof(o.project) === 'undefined') {
        throw new Error('Cloud Project not found, run earthengine authenticate command or provide project name using the -p | --project argument')
      } else {
        project = o.project;
      }
    }

    // opt_baseurl, opt_tileurl, opt_successCallback, opt_errorCallback, opt_xsrfToken, opt_project
    ee.initialize(null, null, () => {
      onsuccess();
    }, (err) => {
      console.error(err);
      throw new Error('Could not initialize EE');
    }, null, project);

    } catch (err) {
      console.error('Could not authenticate with Application Default Credentials:', err);
      throw err;
    }
  })();
};

module.exports.initialize = initialize;

// initialize()
