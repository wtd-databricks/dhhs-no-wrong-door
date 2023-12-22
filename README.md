# dhhs-no-wrong-door
Demo made for DHHS's to showcase Databricks's capability to handle the No Wrong Door policy 

## Project Structure
- `src/`: Contains the Python source files
  - `generate_historical_applications_json.py`: This script generates 2.5M DHHS applications in the form of JSON. Categories of applications include Income, Health Care Assistance, Coping With Crisis, Child Care Assistance, Housing, and Food & Nutrition. The script creates the applications in their original_category, and then reroutes them based off hardcoded logic to their rerouted_category. They are then accepted and declined in their respective rerouted_category based off decision logic. This mock dataset does not fully reflect ethical considerations, as it was meant just to generate historical mock data of what a DHHS may have on their historical applications.
- `data/`: Directory where the extracted data files are stored.
- `requirements.txt`: Lists all the Python dependencies required for the project.
- `.gitignore`: Specifies files and directories that Git should ignore.

## Getting Started
While intended to be used in both notebook/online/remote and "traditional"/offline/local development, this module is specifically structured for local development and as such requires a virtual environment.

- Navigate to the cloned repository directory:
  cd your-repository
- If there is a requirements.txt file, set up a virtual environment and install the required Python packages:
  - Create a virtual environment:
    `python -m venv venv`
  - Activate the virtual environment:
    - On macOS/Linux:
      `source venv/bin/activate`
    - On Windows:
      `venv\Scripts\activate`
  - Install dependencies:
    `pip install -r requirements.txt`

## Usage - Notebook/Online/Remote
To use this module in your notebook add to the first command the following call:<br/>
```%pip install git+https://github.com/wtd-databricks/no-wrong-door```
