Catalog App Project
=============
What is it?

The program uses a database created with python and sqlalchemy which interacts with flask for HTML to  get access to the the database. This project displays a a ctalog application where the user can see the latest items. Also it can add new categories and add items to each category.

How it does it?

The database is created with pythonand sqlalchemy, flask is used for access to the database and displaying with HTML. The programs is accessed within the vagrant machine. Connect to the localhost on port 5000 the user will come to the main page where it will be able to see the latest items and their description. In order to do changes the user will need to log in using a google sign procedure. After log in the user will be able to add, edit and delete items.

Software requirements?
    1. Python 3
    2. Vagrant
    3. catalog.py
    4. catalogApp.py
    5. Google account

How to Create OAUTH file?

1. Obtain OAuth 2.0 credentials from the Google API Console.
Visit the Google API Console to obtain OAuth 2.0 credentials such as a client ID and client secret that are known to both Google and your application. The set of values varies based on what type of application you are building. For example, a JavaScript application does not require a secret, but a web server application does.

2. Obtain an access token from the Google Authorization Server.
Before your application can access private data using a Google API, it must obtain an access token that grants access to that API. A single access token can grant varying degrees of access to multiple APIs. A variable parameter called scope controls the set of resources and operations that an access token permits. During the access-token request, your application sends one or more values in the scope parameter.

There are several ways to make this request, and they vary based on the type of application you are building. For example, a JavaScript application might request an access token using a browser redirect to Google, while an application installed on a device that has no browser uses web service requests.

Some requests require an authentication step where the user logs in with their Google account. After logging in, the user is asked whether they are willing to grant the permissions that your application is requesting. This process is called user consent.

If the user grants the permission, the Google Authorization Server sends your application an access token (or an authorization code that your application can use to obtain an access token). If the user does not grant the permission, the server returns an error.

It is generally a best practice to request scopes incrementally, at the time access is required, rather than up front. For example, an app that wants to support purchases should not request Google Wallet access until the user presses the “buy” button; see Incremental authorization.

3. Send the access token to an API.
After an application obtains an access token, it sends the token to a Google API in an HTTP authorization header. It is possible to send tokens as URI query-string parameters, but we don't recommend it, because URI parameters can end up in log files that are not completely secure. Also, it is good REST practice to avoid creating unnecessary URI parameter names.

Access tokens are valid only for the set of operations and resources described in the scope of the token request. For example, if an access token is issued for the Google+ API, it does not grant access to the Google Contacts API. You can, however, send that access token to the Google+ API multiple times for similar operations.

4. Refresh the access token, if necessary.
Access tokens have limited lifetimes. If your application needs access to a Google API beyond the lifetime of a single access token, it can obtain a refresh token. A refresh token allows your application to obtain new access tokens.

5. File will then needs to be saved on the same folder where the vagrant file is located.
6. You will need to paste the Client ID on the login.HTML page at "data-client-id"

How to run it?

User need to download the following latest Python 3 version, Vagrant and setup the Virtual Machine with catalogApp.py. 

Please follow the following instructions:

1. Install Vagrant and VirtualBox
2. Clone the fullstack-nanodegree-vm
3. Launch the Vagrant VM (vagrant up)
4. Write your Flask application locally in the vagrant/catalog directory (which will automatically be synced to /vagrant/catalog within the VM).
5. Run your application within the VM (python /vagrant/catalog/application.py)
Access and test your application by visiting http://localhost:5000 locally