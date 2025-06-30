import pandas as pd
 
def transformed_data(documents):
    df=pd.DataFrame(documents) 
    df_flat2=pd.json_normalize(df.to_dict(orient='records'),sep='_')
    df_flat2['technologies']=df_flat2['technologies'].apply(lambda x: ",".join(x)if isinstance(x,list) else '')
    df_flat2['milestones'] = df_flat2['milestones'].apply(lambda x: x if isinstance(x, list) else [])
 
    df_milestones = df_flat2[['project_id', 'milestones']].explode('milestones')
 
    df_milestones = pd.concat([
        df_milestones.drop(['milestones'], axis=1),
        df_milestones['milestones'].apply(pd.Series).rename(columns={
            'name': 'milestone_name',
            'due_date': 'milestone_due_date'
        })
    ], axis=1) 
 
 
    df_final = pd.merge(df_flat2.drop(columns=['milestones']), df_milestones, on='project_id', how='left')
 
 
    df_members = df[['project_id', 'team']].copy()
    df_members['members'] = df_members['team'].apply(lambda x: x.get('members', []))
    df_members = df_members[['project_id', 'members']].explode('members')
    df_members = pd.concat([
        df_members.drop(['members'], axis=1),
        df_members['members'].apply(pd.Series).rename(columns={'name':'team_member_name','role':'team_member_role'})
    ], axis=1)
 
    df_final2=pd.merge(df_final.drop(columns=['team_members']),df_members,on='project_id',how='left' )
 
 
    return df_final2






