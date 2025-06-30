import pandas as pd

def transform(documents):
    projects_data = []
    milestones_data = []
    team_members_data = []

    for project in documents:
        project_record ={
            "project_id": project["project_id"],
            "project_name": project["project_name"],
            "technologies": ", ".join(project["technologies"]),
            "status": project["status"],
            "client_name": (
                project["client"]["client"]["name"]
                if "client" in project and "client" in project["client"]
                else project["client"]["name"]
            ),
            "client_industry": project["client"]["industry"],
            "client_city": project["client"]["location"]["city"],
            "client_country": project["client"]["location"]["country"],
            "project_manager": project["team"]["project_manager"],
        }
        projects_data.append(project_record)

        # Milestones
        for milestone in project.get("milestones", []):
            milestones_data.append({
                "project_id": project["project_id"],
                "milestone_name": milestone["name"],
                "due_date": milestone["due_date"],
            })

        # Team Members
        for member in project["team"].get("members", []):
            team_members_data.append({
                "project_id": project["project_id"],
                "member_name": member["name"],
                "member_role": member["role"],
            })

    df_projects = pd.DataFrame(projects_data)
    df_milestones = pd.DataFrame(milestones_data)
    df_team_members = pd.DataFrame(team_members_data)

    return df_projects, df_milestones, df_team_members
