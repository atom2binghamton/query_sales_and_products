# AI-Powered SQL Query Assistant

## Supporting Services

1. Create a postgres database on [render.com](https://dashboard.render.com/)
2. [Buy tokens](https://platform.openai.com/settings/organization/billing/overview)
3. [Generate a API key](https://platform.openai.com/settings/organization/api-keys)
   

## Steps to run

1. Clone repo using `git clone`.
2. Create a `.streamlit` folder with a `secrets.toml` file (for use by streamlit) and a `.env` file (for your local environment).
3. Paste the contents of the filled out `sample.env` file inside of both.
4. Create a new python environment (using Conda). `conda create -n <env_name> python=3`
5. Activate environment. `conda activate <env_name>`
6. Install packages. `pip install -r requirements.txt`
7. Input a strong password that will be hashed by the Python script. `python generate_password.py`
8. Run database test. `python test_render_database.py`
9. Populate database. `python populate_db.py`
10. Run Streamlit app. `streamlit run streamlit_app.py`
11. Log in with the password you used for generating the hash earlier.
12. Ask questions using plain English and test the queries given by ChatGPT.


## How create hashed password

```python
import bcrypt
password = "some_strong_password".encode('utf-8')
hashed = bcrypt.hashpw(password, bcrypt.gensalt())
print(hashed.decode())
```