
# coding: utf-8

# ## Purpose: add new events to Canon of Event Metrics tool

# ### General steps:
# 
# #### <font color='gray'>1. Create event in QGI db </font> 
# <font color='gray'>-create 3 column csv with above data  </font>  
# <font color='gray'>-read into jupyter  </font>  
# <font color='gray'>-iterate through and insert events in qgi db  </font>  
# <font color='gray'>2. Populate event with list of users  </font>   
# <font color='gray'>    -insert new event ids into users csv  </font>   
# <font color='gray'>    -read in csv in jupyter  </font>   
# <font color='gray'>    -iterate through and insert each user into event_users table  </font>   
# <font color='gray'> 3. Run 4 acts247 db queries against QGI db event  </font>  
# <font color='gray'> 4. Store each result (or 1 result if stored procedure is written) into existing excel wb  </font>

# ### 1. Create event in QGI db

# In[2]:

##########################
### Needed Libraries ####

import pymysql
import csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import dateutil.parser
import time
import datetime


# In[3]:

####################
### Connect to DEV db ##
#for insertion of events ##


## 2 dev db connections needed to test
password = open("C:\\Users\\brian.preisler\\Dropbox\\Growth\\Data Analysis\\Event Analysis\\Master Canon of Event Metrics\\Python Data Pulls\\qgi_db_password.txt","r")  
pw = password.read()  
dev1_db=pymysql.connect( host="localhost",
                        user="python",
                        passwd= pw,
                        db="qgi",
                        charset='utf8',
                        use_unicode=True)
dev2_db=pymysql.connect( host="localhost",
                        user="python",
                        passwd=pw,
                        db="acts247",
                        charset='utf8',
                        use_unicode=True)


#2 live db connections needed to run queries
password2 = open("C:\\Users\\brian.preisler\\Dropbox\\Growth\\Data Analysis\\Event Analysis\\Master Canon of Event Metrics\\Python Data Pulls\\acts_db_pw.txt","r")  
pw2 = password2.read()  
live1_db=pymysql.connect( host="acts247.focus.org",
                        user="qgi",
                        passwd=pw2,
                        db="qgi",
                        charset='utf8',
                        use_unicode=True)

live2_db=pymysql.connect( host="acts247.focus.org",
                        user="qgi",
                        passwd=pw2,
                        db="acts247",
                        charset='utf8',
                        use_unicode=True)


# In[4]:

#########################
### Custom params ######
### for entire script ##

DB1 = live1_db #(Options: dev1_db, live1_db)
DB2 = live2_db #(Options: dev2_db, live2_db)

cursor1 = DB1.cursor()
cursor2 = DB2.cursor()


# Choose type of event
type_id = 1
#1	Missions
#3	Spiritual Impact Bootcamp
#4	Summer Projects
#5	Fathers House
#6	SEEK
#7	NST
#8	SLS

#____________________________

#Filepath of event user csv (follow below steps to prep):
# 1. CSV has all user ids that attended events
# 2. Remove any CANCELLED events or event names with odd characters (accents, etc.)
# 3. Remove STAFF members
event_user_csv_test = 'C:\\Users\\brian.preisler\\Dropbox\\Growth\\Data Analysis\\Event Analysis\\Master Canon of Event Metrics\\Python Data Pulls\\all_mission_participants_and_acts_IDs_test.csv'

event_user_csv_full = 'C:\\Users\\brian.preisler\\Dropbox\\Growth\\Data Analysis\\Event Analysis\\Master Canon of Event Metrics\\Python Data Pulls\\all_mission_participants_and_acts_IDs.csv'

event_user_csv = event_user_csv_full

#______________________________

#Filepath of final dataset csv from all 4 queries
day = time.strftime("%m-%d-%Y")
file_name = 'event_dataset_' + day + '.csv'
file_path = 'C:\\Users\\brian.preisler\\Dropbox\\Growth\\Data Analysis\\Event Analysis\\Master Canon of Event Metrics\\Python Data Pulls\\' + file_name



# In[5]:

raw_event_df = pd.DataFrame()
raw_event_df = pd.read_csv(event_user_csv)

print raw_event_df


# In[6]:

events_df = pd.DataFrame()
events_df = raw_event_df.ix[0:,('event', 'start_date', 'end_date')].copy()

print 'Number of students in Raw Event df: ',len(events_df)

events_df = events_df.drop_duplicates()

events_df = events_df.dropna()

print 'Unique events derived from Raw Event df: ', len(events_df)

events_df.sort('event')


# In[7]:

#########################
#Place events_df into db#
########################

for index,row in events_df.iterrows():
    #print row
    event_name = row['event']
    start_date = dateutil.parser.parse(row['start_date']).date()
    end_date = dateutil.parser.parse(row['end_date']).date()

                
    query= """
          insert into event(name, type_id, start_date, end_date)
          values (%s, %r, %s, %s) 
           """
    
    result = cursor1.execute(query, (event_name, type_id, start_date, end_date))

    DB1.commit()
print 'events placed into acts247 db'


# In[9]:

############################################
#Pull event ids from db, join with users db#
############################################

query= """
          select event.id, event.name, event.start_date, event.end_date
          from event
           """

result = cursor1.execute(query)
result = cursor1.fetchall()

#create events_id df from db to match up with users
events_id_df = pd.DataFrame(list(result),columns = ['event_id', 'event', 'start_date', 'end_date'])


#merge new event_ids onto users df
event_users_df = raw_event_df.ix[0:,('acts247_user_id', 'event','start_date' )].copy()

print 'starting amount of users before removing any: ', len(event_users_df)

#change datetypes so that join can happen on consistent datetimes
event_users_df['start_date'] = pd.to_datetime(event_users_df.start_date)
events_id_df['start_date'] = pd.to_datetime(events_id_df.start_date)
events_id_df['end_date'] = pd.to_datetime(events_id_df.end_date)

#merge 2 df together on multiple criteria to avoid mistakes with annual events with similar names
event_users_df = pd.merge(event_users_df, events_id_df, how='inner', on=['event', 'start_date'])

#check users before dropping those with no user_id in db

event_users_df = event_users_df.dropna(subset=['acts247_user_id'])

event_users_df = event_users_df.drop_duplicates(subset=['event', 'start_date', 'end_date', 'acts247_user_id'])

#check users left after dropping 
print 'users left after keeping only those with ids and removing dups: ' , len(event_users_df)

print ""

#change datatype of id
event_users_df.acts247_user_id = event_users_df.acts247_user_id.astype(int)

#events_id_df.dtypes

print "List of users to go through Canon queries:"
print ""
print event_users_df


# In[10]:

#print events_df

events_df['start_date'] = pd.to_datetime(events_df.start_date)
events_df['end_date'] = pd.to_datetime(events_df.end_date)

#print events_df

#print events_id_df

events_df = pd.merge(events_df, events_id_df, how='inner', on=['event', 'start_date', 'end_date'])

print "Event df with Acts event_id added:"

events_df


# ### Prep and Enter Event User Dataframe into Database
# 

# In[11]:

####################################################
#add users ids and event ids into event users table#
####################################################


for index,row in event_users_df.iterrows():
    user_id = row['acts247_user_id']
    event_name = row['event']
    event_id = row['event_id']
    #print event_name, row['acts247_user_id']
    start_date = row['start_date']
    end_date = row['end_date']
    
    query= """
          insert into event_users(user_id, event_id)
          values (%s, %s) 
           """
    result = cursor1.execute(query, (user_id, event_id))
    
    DB1.commit()
    
print "Event users added to qgi db in event_users table"


# ### Call 4 Canon Queries:  
# -iterate through list of event_ids  
# -call canon queries against event event  
# -write query result to csv  
# 

# In[12]:

#define function to choose correct 3 month mark before date

def before_dates(x):
    if x.month in range(5,10): #for months 5,6,7,8,9 (but not 10)
        three_month_before_date = str(x.year) + '-02-01'
        return three_month_before_date
    if x.month == 10:
        three_month_before_date = str(x.year) + '-03-01'
        return three_month_before_date
    if x.month == 11:
        three_month_before_date = str(x.year) + '-04-01'
        return three_month_before_date
    if x.month == 12:
        three_month_before_date = str(x.year) + '-09-01'
        return three_month_before_date
    if x.month in range(1,3):
        last_year = int(x.year) - 1
        three_month_before_date = str(last_year) + '-09-01'
        return three_month_before_date
    if x.month == 3:
        last_year = int(x.year) - 1
        three_month_before_date = str(last_year) + '-10-01'
        return three_month_before_date
    if x.month == 4:
        last_year = int(x.year) - 1
        three_month_before_date = str(last_year) + '-11-01'
        return three_month_before_date
     
        
#Return 3 'months' after the date input        
        
def after_dates(x):
    if x.month in range(4,9): #for months 4,5,6,7,8 (but not 10)
        three_month_after_date = str(x.year) + '-12-01'
        return three_month_after_date
    if x.month == 9:
        new_year = int(x.year + 1)
        three_month_after_date = str(new_year) + '-02-01'
        return three_month_after_date
    if x.month == 10:
        new_year = int(x.year + 1)
        three_month_after_date = str(new_year) + '-03-01'
        return three_month_after_date
    if x.month == 11:
        new_year = int(x.year + 1)
        three_month_after_date = str(new_year) + '-04-01'
        return three_month_after_date
    if x.month == 12:
        new_year = int(x.year + 1)
        three_month_after_date = str(new_year) + '-05-01'
        return three_month_after_date
    if x.month in range(1,3):
        three_month_after_date = str(x.year) + '-10-01'
        return three_month_after_date
    if x.month == 3:
        three_month_after_date = str(x.year) + '-11-01'
        return three_month_after_date

#Test dates to ensure function works properly
date = dateutil.parser.parse('10/3/2015')
after_dates(date)

#function to return fiscal year
def fiscal_year(x):
    if x.month in range(6,13): #for months 6,7,8,9,10,11,12
        fy = int(x.year) + 1
        return fy
    if x.month in range(1,6):
        fy = int(x.year)
        return fy

fiscal_year(date)


# In[13]:

print len(events_df)

counter = 0

for index,row in events_df.iterrows():
    event = row['event_id']
    counter = counter + 1
    print row, counter


# In[14]:

#initiate dfs needed to store user level query results   
df_master = pd.DataFrame(data = None, columns = ['event_id', 'user_id', 'first', 'last', 'year', 'campus', 
                                            'earliest_BSA', 'count_BSAtt', 'count_BSAb',
                                          'Was_D_at_start', 'GP_outcome'] )
df_p2 = pd.DataFrame(data = None, columns = ['user_id', 'earliest_BS_atten2', 'Attended2', 'Absent2',
                                              'Was_D_3_mo_After2', 'GP_outcome_post2'] )

df_l1 = pd.DataFrame(data = None, columns = ['user_id', 'first_became_disciple', 'earliest_led_bs',
                                                       'first_became_DM', 'count_of_DCC', 'latest_change_to_DC',
                                                       'slbs_in_chain',
                                                       'number_of_D_at_start', 'has_second_genderation',
                                                       'last_log_in'])

df_l2 = pd.DataFrame(data = None, columns = ['user_id','first_became_disciple2', 'earliest_led_bs2',
                                                       'first_became_DM2', 'count of DCC2', 'latest_change_to_DC2',
                                                        'slbs_in_chain2',
                                                       'number_of_D_3_mo_later2', 'has_second_generation2',
                                                       'last_log_in2'])

#force df to set certain datatypes in columns
df_master.event_id = df_master.event_id.astype(int)
df_master.user_id = df_master.user_id.astype(int)
df_master.count_BSAtt = df_master.count_BSAtt.astype(int)
df_master.count_BSAb = df_master.count_BSAb.astype(int)
df_master.Was_D_at_start = df_master.Was_D_at_start.astype(int)

#for loop to run P1,P2,L1,L2 Canon Query on events listed in csv

counter = 0

for index,row in events_df.iterrows():
    event_id = row['event_id']
    start_date = row['start_date'].date()
    end_date = row['end_date'].date()
    three_month_before_date = before_dates(start_date)
    day_before_event_date = start_date
    three_month_after_date = after_dates(end_date)
    day_after_event_date = end_date
    FY = fiscal_year(end_date)

    query = """
                select qgi.E.name as 'event_name', 
                qgi.ET.name as 'event_type',
                qgi.E.start_date,
                qgi.E.end_date,
                U.id, 
                U.first_name,
                U.last_name, 		
                    case when U.school_year_id = 1 then 'Freshman' when U.school_year_id = 2 then 'Sophomore' when U.school_year_id = 3 then 'Junior'	
                        when U.school_year_id = 4 then 'Senior' when U.school_year_id = 5 then 'Senior+' when U.school_year_id = 6 then 'Grad Student' end as 'School Year', 
                    C.name, 
                
                    earliest_bs_attendance_date(U.id), 		
                    count(distinct case when BSAU.attended = 1 and BSA.date between %s and %s then BSAU.id end) as 'Attended',	
                    count(distinct case when BSAU.attended = 0 and BSA.date between %s and %s then BSAU.id end) as 'Absent',	
                    case when D.id IS NULL then 0 when D.id IS NOT NULL then 1 end as 'Was a Disciple at Start of Event?', 		
                    case when GPR.outcome = 1 then '1' when GPR.outcome = 0 then '0' end as 'GP Outcome'		

                from users as U 		
                join campuses as C on C.id = U.campus_id		
                join qgi.event_users as EU on (EU.user_id = U.id and EU.event_id = %s) #join the table Test which contains all event attendees IDs
                join qgi.event as E on E.id = EU.event_id
                join qgi.event_type as ET on ET.id = E.type_id

                #to count # of BS attended in 3mo before event as well as number of absent
                left join bible_study_attendances_users as BSAU on BSAU.user_id = U.id		
                left join bible_study_attendances as BSA on (BSA.id = BSAU.bible_study_attendance_id and BSA.date between %s and %s)		

                #to see if student was a disciple 3mo before event
                left join disciples as D on (D.user_id = U.id and (D.start_date <= %s and (D.end_date > %s or D.end_date IS NULL)))		

                #outcome of Gospel Presentation if student ever received it
                left join gospel_presentations_recipients as GPR on GPR.user_id = U.id		

                where #C.region_id NOT IN (10,26,44,45)		#regions: miscellaneous, alumni, non-FOCUS, Parishes
                U.user_role_type_id NOT IN (4,6)		#role type: missionary and TD; to ensure only student fruitfulness is returned
                and BSA.cancel_reason_id IS NULL		
                #and U.school_year_id IN (1,2,3,4,5,6) 		#sy: freshman - senior, senior+, grad student; to make sure alumni and non-student are not counted
                Group by U.id
                """

    result3 = cursor2.execute(query, 
        (three_month_before_date, 
         day_before_event_date, 
         three_month_before_date, 
         day_before_event_date,
         event_id,
         three_month_before_date,
         day_before_event_date,
         day_before_event_date,
         day_before_event_date
        ))
    p1_output = cursor2.fetchall()
    
    
    #start of p2 data gather:
    
    query2 = """
            Select
                U.id as 'user_id',
                earliest_bs_attendance_date(U.id), 		
                count(distinct case when BSAU.attended = 1 and BSA.date between %s and %s then BSAU.id end) as 'Attended',	
                count(distinct case when BSAU.attended = 0 and BSA.date between %s and %s then BSAU.id end) as 'Absent',	
                case when D.id IS NULL then '0' when D.id IS NOT NULL then '1' end as 'Was a Disciple at 3 Months After Event?', 		
                case when GPR.outcome = 1 then '1' when GPR.outcome = 0 then '0' end as 'GP Outcome'		

            from users as U 		
            join campuses as C on C.id = U.campus_id		
            join qgi.event_users as EU on (EU.user_id = U.id and EU.event_id = %s) #join the table Test which contains all event attendees IDs

            #to count # of BS attended in 3mo after event as well as number of absent
            left join bible_study_attendances_users as BSAU on BSAU.user_id = U.id		
            left join bible_study_attendances as BSA on (BSA.id = BSAU.bible_study_attendance_id and BSA.date between %s and %s and BSA.cancel_reason_id IS NULL)		

            #to see if student was a disciple 3mo after event
            left join disciples as D on (D.user_id = U.id and (D.start_date <= %s and (D.end_date > %s or D.end_date IS NULL)))		

            #outcome of Gospel Presentation if student ever received it
            left join gospel_presentations_recipients as GPR on GPR.user_id = U.id		

            where C.region_id NOT IN (10,26,44,45)	 #regions: miscellaneous, alumni, non-FOCUS, Parishes	
            and U.user_role_type_id NOT IN (4,6)	 #role type: missionary and TD; to ensure only student fruitfulness is returned	
            and BSA.cancel_reason_id IS NULL		
            and U.school_year_id IN (1,2,3,4,5,6)  	 #sy: freshman - senior, senior+, grad student; to make sure alumni and non-student are not counted	
            group by U.id"""
    
    result4 = cursor2.execute(query2, 
        (day_after_event_date,  #1
         three_month_after_date,#2
         day_after_event_date,  #3
         three_month_after_date,#4
         event_id,              #5
         day_after_event_date,  #8
         three_month_after_date,#9
         three_month_after_date,#10
         three_month_after_date #11
        ))
    p2_output = cursor2.fetchall()

    
    #start of l1 data gather: 
    queryl1 = """
                SELECT
                U.id as 'user_id', 
                earliest_discipleship_start_date(U.id) as 'First Became Disciple', 
                SLBS.min as 'Earliest Led BS',	
                DM.min as 'First Became DM', Depth.count as 'Count of DCC', Depth.latest 'Latest Change to DC',	
                 P.count as 'SLBS in chain', DM.count as 'Number of D at start of event', 					
                SG.sg as 'Has a Second Gen?', U.last_login as 'Last Log-in'		

            from users as U 											
            join campuses as C on C.id = U.campus_id								
            join qgi.event_users as EU on (EU.user_id = U.id and EU.event_id = %s)  #join the table which contains all event attendees IDs and update event_id to appropriate id

            #to find First Became DM
            left join (select U.id as 'id', min(D.start_date) as 'min', count(distinct case  when D.start_date <= %s and (D.end_date >= %s OR D.end_date IS NULL) then D.user_id end) as 'count'
                from users as U
                join disciples as D on (D.parent_user_id = U.id)
                group by U.id) as DM on DM.id = U.id 																	
            #to find Earliest Led BS
            left join (select U.id as 'id', min(BSS.study_start_date) as 'min'						
                from users as U											
                join bible_studies as BS on U.id = BS.leader_id						
                join bible_study_schedules as BSS on (BSS.bible_study_id = BS.id ) 									
                group by U.id) as SLBS on SLBS.id = U.id																	

            #to find if user had a second gen (SG) before the event
            left join (select D1.user_id as 'id', case when D3.user_id IS NULL then 0 when D3.user_id IS NOT NULL then 1 end as 'sg'											
                from disciples as D1										
                left join disciples as D2 on D1.user_id = D2.parent_user_id					
                left join disciples as D3 on D2.user_id = D3.parent_user_id					
                where (D1.end_date IS NULL or D1.end_date >= %s)
                and (D2.end_date IS NULL or D2.end_date >= %s)			
                and (D3.end_date IS NULL or D3.end_date >= %s)			
                group by D1.user_id) as SG on SG.id = U.id																	

            #to count Depth Chart Contacts (DCC) and see when was latest activity before event
            left join (select O.id as 'id', count(distinct DC.chart_member_id) as 'count', max(DC.start_date) as 'latest'	
                from users as O											
                join depth_chart as DC on (DC.chart_owner_id = O.id and DC.start_date <= %s and (DC.end_date IS NULL OR DC.end_date >= 	%s))
                group by O.id) as Depth on Depth.id = U.id																	
																	
            #to find SLBS in chain before event
            left join (SELECT  P.id as' id', count(distinct BS.leader_id) as 'count'					from bible_studies as BS									
                JOIN bible_study_schedules as BSS on ((BSS.bible_study_id = BS.id) AND (BSS.is_latest = 1))	
                JOIN users as U on U.id = BS.leader_id								
                left join disciples 			as D on ((D.user_id = BS.leader_id) and ((D.end_date IS NULL) OR (D.end_date >= %s)))						
                left join users 				as P on P.id = D.parent_user_id				
                WHERE BS.deleted = 0										
                AND BSS.study_start_date <= %s			
                AND (BSS.study_end_date IS NULL OR (BSS.study_end_date >= %s))	
                group by P.id) as P on P.id = U.id								

            where C.region_id NOT IN (10,26,44,45)	#regions: miscellaneous, alumni, non-FOCUS, Parishes		
            and U.user_role_type_id NOT IN (4,6)	 #role type: missionary and TD; to ensure only student fruitfulness is returned									
            and U.school_year_id IN (1,2,3,4,5,6)	#sy: freshman - senior, senior+, grad student; to make sure alumni and non-student are not counted								
            group by U.id"""
    
    result5 = cursor2.execute(queryl1, 
        (event_id,             #1
         day_before_event_date,#2
         day_before_event_date, #3
         day_before_event_date, #4
         day_before_event_date, #5
         day_before_event_date, #6
         day_before_event_date, #7
         day_before_event_date, #8
         day_before_event_date, #13
         day_before_event_date, #14
         day_before_event_date #15
        ))
    l1_output = cursor2.fetchall()

    
#start of l2 data gather:    
    queryl2 = """select 
                U.id as 'user_id', 
                earliest_discipleship_start_date(U.id) as 'First Became Disciple', SLBS.min as 'Earliest Led BS',					
                DM.min as 'First Became DM', Depth.count as 'Count of DCC', Depth.latest 'Latest Change to DC',					
                P.count as 'SLBS in chain', DM.count as 'Number of D 3 Months After Event', 					
                SG.sg as 'Has a Second Gen?', U.last_login as 'Last Log-in'

                from users as U
                join campuses as C on C.id = U.campus_id
                join qgi.event_users as EU on(EU.user_id = U.id and EU.event_id = %s) #join the table which contains all event attendees IDs and update event_id to appropriate id

                #to find First Became DM				
                left join (select U.id as 'id', min(D.start_date) as 'min', count(distinct case  when D.start_date <= %s and (D.end_date >= %s OR D.end_date IS NULL) then D.user_id end) as 'count'
                    from users as U
                    join disciples as D on (D.parent_user_id = U.id)
                    group by U.id) as DM on DM.id = U.id 

                #to find Earliest Led BS
                left join (select U.id as 'id', min(BSS.study_start_date) as 'min'					
                    from users as U				
                    join bible_studies as BS on U.id = BS.leader_id				
                    join bible_study_schedules as BSS on (BSS.bible_study_id = BS.id) 				
                    group by U.id) as SLBS on SLBS.id = U.id				

                #to find if user has a second gen (SG)
                left join (select D1.user_id as 'id', case when D3.user_id IS NULL then 0 when D3.user_id IS NOT NULL then 1 end as 'sg'					
                    from disciples as D1				
                    left join disciples as D2 on D1.user_id = D2.parent_user_id				
                    left join disciples as D3 on D2.user_id = D3.parent_user_id				
                    where (D1.end_date IS NULL or D1.end_date >= %s)				
                    and (D2.end_date IS NULL or D2.end_date >= %s)				
                    and (D3.end_date IS NULL or D3.end_date >= %s)				
                    group by D1.user_id) as SG on SG.id = U.id				

                #to count Depth Chart Contacts (DCC)
                left join (select O.id as 'id', count(distinct DC.chart_member_id) as 'count', max(DC.start_date) as 'latest'					
                    from users as O				
                    join depth_chart as DC on (DC.chart_owner_id = O.id and DC.start_date <= %s and (DC.end_date IS NULL OR DC.end_date >= %s))				
                    group by O.id) as Depth on Depth.id = U.id						

                #to find SLBS in chain
                left join (SELECT  P.id as' id', count(distinct BS.leader_id) as 'count'					
                    from bible_studies as BS				
                    JOIN bible_study_schedules as BSS on ((BSS.bible_study_id = BS.id) AND (BSS.is_latest = 1))				
                    JOIN users as U on U.id = BS.leader_id				
                    left join disciples 			as D on ((D.user_id = BS.leader_id) and ((D.end_date IS NULL) OR (D.end_date >= %s)))	
                    left join users 				as P on P.id = D.parent_user_id
                    WHERE BS.deleted = 0				
                    AND BSS.study_start_date <= %s				
                    AND (BSS.study_end_date IS NULL OR (BSS.study_end_date >= %s))				
                    group by P.id) as P on P.id = U.id				

                where C.region_id NOT IN (10,26,44,45) #regions: miscellaneous, alumni, non-FOCUS, Parishes					
                and U.user_role_type_id NOT IN (4,6) #role type: missionary and TD; to ensure only student fruitfulness is returned					
                and U.school_year_id IN (1,2,3,4,5,6) #sy: freshman - senior, senior+, grad student; to make sure alumni and non-student are not counted
                group by U.id
               """
    
    result6 = cursor2.execute(queryl2, 
        (event_id,              #1
         three_month_after_date,#2
         three_month_after_date, #3
         three_month_after_date, #4
         three_month_after_date, #5
         three_month_after_date, #6
         three_month_after_date, #7
         three_month_after_date, #8
         three_month_after_date, #13
         three_month_after_date, #14
         three_month_after_date #15
        ))
    l2_output = cursor2.fetchall()
    
    
    
    #place outputs of both queries into temporary dfs 
    df_temp = pd.DataFrame(list(p1_output),columns = ['event_name', 'event_type', 'start_date', 'end_date', 'user_id', 
                                                      'first', 'last', 'year', 'campus', 'earliest_BSA',
                                                      'count_BSAtt', 'count_BSAb','Was_D_at_start', 'GP_outcome'] ) #15 cols
    
    df_temp2 = pd.DataFrame(list(p2_output),columns = ['user_id','earliest_BS_atten2', 'Attended2', 'Absent2',
                                              'Was_D_3_mo_After2', 'GP_outcome_post2'] ) #6 cols after join
                                          
    df_temp3 = pd.DataFrame(list(l1_output), columns = ['user_id', 'first_became_disciple', 'earliest_led_bs',
                                                       'first_became_DM', 'count_of_DCC', 'latest_change_to_DC',
                                                       'slbs_in_chain',
                                                       'number_of_D_at_start', 'has_second_generation',
                                                       'last_log_in']) #11 cols after join
    
    df_temp4 = pd.DataFrame(list(l2_output), columns = ['user_id','first_became_disciple2', 'earliest_led_bs2',
                                                       'first_became_DM2', 'count of DCC2', 'latest_change_to_DC2',
                                                        'slbs_in_chain2',
                                                       'number_of_D_3_mo_later2', 'has_second_generation2',
                                                       'last_log_in2']) #11 cols after join (43 total)

    
    #Place P1 results in master dataframe
    df_master = df_master.append(df_temp)
    
    df_master['3_mo_after'] = df_master.apply(lambda row: three_month_after_date, axis=1 )
    df_master['FY'] = df_master.apply(lambda row: FY, axis=1 )
    
    #Place P2 results in P2 dataframe
    df_p2 = df_p2.append(df_temp2)
    
    #Place L1 results in L1 dataframe
    df_l1 = df_l1.append(df_temp3)
    
    #Place l2 results in L2 dataframe
    df_l2 = df_l2.append(df_temp4)
    
    #Reset index of master df
    df_master = df_master.reset_index(drop=True)
    
    counter = counter + 1
    print counter, row
    
    #print p1_output


# In[15]:

print df_master


# In[14]:

print df_p2


# In[16]:

#Merge P2,L1,L2 onto master using user_id
df_master = pd.merge(df_master, df_p2, on = 'user_id')
df_master = pd.merge(df_master, df_l1, on = 'user_id')
df_master = pd.merge(df_master, df_l2, on = 'user_id')

col = [#p1
                                              'event_name','event_type', 'start_date', 'end_date', '3_mo_after', 'FY',
                                              'user_id', 'first','last', 'year', 'campus', 'earliest_BSA',
                                              'count_BSAtt', 'count_BSAb', 'Was_D_at_start', 'GP_outcome',
                                              #p2
                                              'earliest_BS_atten2', 'Attended2', 'Absent2',
                                              'Was_D_3_mo_After2', 'GP_outcome_post2',
                                              #l1
                                                'first_became_disciple', 'earliest_led_bs',
                                               'first_became_DM', 'count_of_DCC', 'latest_change_to_DC',
                                               'slbs_in_chain',
                                               'number_of_D_at_start', 'has_second_generation',
                                               'last_log_in',
                                                #l2
                                                'first_became_disciple2', 'earliest_led_bs2',
                                               'first_became_DM2', 'count of DCC2', 'latest_change_to_DC2',
                                               'slbs_in_chain2',
                                               'number_of_D_3_mo_later2', 'has_second_generation2',
                                               'last_log_in2']

#reorder df to match needed input of Canon
df_master = pd.DataFrame(df_master,columns = col)

df_master = df_master.drop_duplicates(subset=['event_name', 'start_date', 'end_date', 'user_id'])

#print df_master.head(900)

df_master.to_csv(file_path, columns = col, encoding='utf-8')#, index = False, header = True, sep = '\t')

df_master.info()


# In[ ]:



