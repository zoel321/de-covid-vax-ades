from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

#reference for template: https://github.com/plotly/dash-sample-apps/tree/main/apps/dash-aix360-heart

#start app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

#import data
stats = pd.read_csv('stats_vis.csv')
ades = pd.read_csv('top_ades.csv')


stats = stats.round(2)
#summary stats for max
index_max = stats.cases.idxmax()
max_row = stats.iloc[index_max]
max_vax = max_row.vax_manu
max_dose_num = max_row.vax_dose_series
max_avg_age = max_row.avg_age
max_std_age = max_row.std_age
max_cases = max_row.cases
max_avg_onset = max_row.avg_onset
max_std_onset = max_row.std_onset
max_location = max_row.v_adminby


#data cleaning for summary data

renamed_dict = {'vax_manu': 'Manufacturer', 'cases': 'Number of Cases',
'vax_dose_series': 'Dose Number', 'v_adminby': 'Administration Location',
'avg_onset': 'Average Onset (Days)', 'avg_age': 'Average Age (Years)',
'std_onset': 'Standard Deviation of Onset (Days)', 'std_age': 'Standard Deviation of Age (Years)'}

stats.rename(columns=renamed_dict, inplace=True)

stats.drop(columns=['Unnamed: 0'], inplace=True)

#data cleaning for symptom data
ades.drop(columns=['Unnamed: 0'], inplace=True)



# summary cards
cards = [
    dbc.Card(
        [
            html.H4(f"{max_vax}, Dose: {max_dose_num}",className="card-title"),
            html.P(f"Location: {max_location}", className="card-text"),
        ],
        body=True,
        color="light",
    ),
    dbc.Card(
        [
            html.H4(f"{max_avg_age} ± {max_std_age}", className="card-title"),
            html.P("Average Age (Years)", className="card-text"),
        ],
        body=True,
        color="light",
    ),
    dbc.Card(
        [
            html.H4(f"{max_avg_onset} ± {max_std_onset}", className="card-title"),
            html.P("Average Onset (Days)", className="card-text"),
        ],
        body=True,
        color="light"
    ),
]



app.layout = dbc.Container([
    html.H1('Covid-19 Vaccine Adverse Events', style={'text-align': 'center'}),
    html.H6('Source: https://vaers.hhs.gov/index.html', style={'text-align': 'center'}),
    html.Br(),

    html.H5(f"Highest number of cases (by manufacturer, vaccine dose number, and administration location): {max_cases}",
        style={'text-align': 'center'}),
    dbc.Row([dbc.Col(card) for card in cards]),

    html.Br(),

    html.H3('Number of Reports per Vaccine', style={'text-align': 'center'}),
    html.H4('Categorize by:'),

    dcc.Dropdown(id='choose-legend',
        options = [
        {'label': 'Dose Number', 'value': 'Dose Number'},
        {'label': 'Administration Location', 'value': 'Administration Location'}],
        multi=False,
        value='Dose Number',
        style={'width': '40%'}),

    html.Div(id='output_response',style={'text-align': 'center'}),
    dcc.Graph(id='bar-graph'),

    html.Br(),
    html.H4('Display adverse event data for:'),
    html.Br(),


    html.H6('Choose manufacturer:'),
    dbc.RadioItems(id ='choose-manu',
            options=[
            {'label': 'Pfizer/BioNTech', 'value':'PFIZER\BIONTECH'},
            {'label': 'Moderna', 'value':'MODERNA'},
            {'label': 'Janssen', 'value': 'JANSSEN'},
            {'label': 'Unknown', 'value': 'UNKNOWN MANUFACTURER'}],
            value='PFIZER\BIONTECH',
            inline=True),

    html.H6('Choose dose number:'),
    dbc.RadioItems(id ='choose-dose',
            options=[
            {'label': '1', 'value':'1'},
            {'label': '2', 'value':'2'},
            {'label': '3', 'value': '3'},
            {'label': '4', 'value': '4'},
            {'label': '5+', 'value': '5+'},
            {'label': 'Unknown', 'value': 'UNK'}],
            value='1',
            inline=True),

    html.H5('Top 10 Symptoms Reported', style={'text-align': 'center'}),
    html.Div(id="ade-table-container"),

    html.Br(),
    html.H5('Summary Table', style={'text-align': 'center'}),
    dash_table.DataTable(
        id='data-table',
        sort_action='native',
        sort_mode='multi',
        tooltip_header={'Average Onset (Days)': 'number of days between vaccination and onset of symptoms'})
    ],
    fluid=False)

# code for callback
@app.callback(
[Output(component_id='output_response', component_property='children'),
 Output(component_id='bar-graph', component_property='figure')],
[Input(component_id='choose-legend', component_property='value')]
)
def update_graph(selection):

    container = "What is the number of cases when grouped by {}?".format(selection)

    fig = px.histogram(
        stats,
        x="Manufacturer",
        y="Number of Cases",
        color=selection,
        hover_data=stats.columns,
        barmode='stack',
        category_orders={"Dose Number": ["1", "2", "3", "4", '5+', 'UNK'],
                         'Manufacturer': ['MODERNA', 'PFIZER\BIONTECH', 'JANSSEN', 'UNKNOWN MANUFACTURER']},
        height=600)

    return container, fig

@app.callback(
[Output(component_id='data-table', component_property='data'),
Output(component_id='data-table', component_property='columns'),
Output(component_id='ade-table-container', component_property='children')],
[Input(component_id='choose-manu', component_property='value'),
Input(component_id='choose-dose', component_property='value')])
def update_tables(manu_selection, dose_selection):

    #for the summary table
    df = stats.copy()
    shorter = df[(df['Manufacturer'] == manu_selection) & (df['Dose Number'] == dose_selection)]

    present_data = shorter.to_dict('records')
    column_data = [{"name": i, "id": i} for i in shorter.columns]

    #for the ade data
    df2 = ades.copy()
    top_ades = ades[(ades.vax_manu== manu_selection) & (ades.vax_dose_series==dose_selection)].sort_values(by=['row'])[["cleaned_symptoms", "symptom_count"]]
    top_ades.rename(columns={"cleaned_symptoms": "Symptom", "symptom_count": "Count"},inplace=True)
    ade_table_input = dbc.Table.from_dataframe(top_ades, striped=True, bordered=True, size='sm')

    return present_data, column_data, ade_table_input



if __name__ == '__main__':
    app.run_server(debug=True)
