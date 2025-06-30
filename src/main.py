from extract import read_config, extract_mongodb
from transform import transform
from transform2 import transformed_data
from load import load_to_sql
def main():
    # Load configuration
    config = read_config()

    # Extract
    documents = extract_mongodb(config)
    print(f"Extracted {len(documents)} documents from MongoDB.")

    # Transform
    df_projects, df_milestones, df_team_members = transform(documents)
    print("Transformation completed.")
    df_final2 = transformed_data(documents)
    print("Transformed data into DataFrames.")

    # Load
    load_to_sql(
        {
            "Projects": df_projects,
            "ProjectMilestones": df_milestones,
            "ProjectTeamMembers": df_team_members,
            "FinalData": df_final2,
        },
        config
    )
    print(" ETL process completed successfully.")

if __name__ == "__main__":
    main()


