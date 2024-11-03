import analysis.visualizations as visualizations
from app.view import *


class LoginPresenter():
    def __init__(self, master, model, view):
        self.master = master
        self.model = model
        self.view = view

    def login(self, event):
        username = self.view.username_entry.get()
        # password = self.view.bar.get()
        login_response = self.model.validate_login(username)
        if login_response['success'] == True:
            refresh_data = self.view.data_refresh_var.get()
            if refresh_data:
                self.model.refresh_sleeper_data()

            self.master.change_view(StartView)
        else:
            # trigger the view.error method and show the login response
            print(login_response['message']) # TODO: Present Error View
            # self.master.error_window(error_message)

    def show_create_account_view(self, event):
        self.master.change_view(CreateAccountView)

    def run(self):
        if self.model.get_user_count() == 0:
            self.master.change_view(CreateAccountView)
        else:
            self.view.init_ui(self)
            # self.view.username_entry.config(values=self.model.get_usernames())
            self.view.grid()

    def exit_view(self):
        self.view.grid_forget()


class CreateAccountPresenter():
    def __init__(self, master, model, view):
        self.master = master
        self.model = model
        self.view = view

    def create_account(self, event):
        username = self.view.username_entry.get()
        # league_id = self.view.league_id_entry.get()
        self.model.create_account(username)
        self.master.change_view(LoginView)

    def show_login_view(self, event):
        self.master.change_view(LoginView)

    def run(self):
        self.view.init_ui(self)
        self.view.grid()

    def exit_view(self):
        self.view.grid_forget()


class StartPresenter():
    def __init__(self, master, model, view):
        self.master = master
        self.model = model
        self.view = view
    def render_graph(self, event):
        visual_selection = self.view.visual_selection.get()
        week = int(self.view.week_selection.get())
        year = 2023

        if visual_selection == 'Schedule Luck':
            luck_factor_df = self.model.get_luck_factor_df(week, year)
            visualizations.luck_factor_plot(luck_factor_df, week)
        elif visual_selection == 'Roster Week Points':
            roster_week_points_df = self.model.get_roster_week_points_df(week, year)
            visualizations.roster_week_points_plot(roster_week_points_df, week)
        else:
            pass

        # if (event != None) and (self.view.canvas != None):
        #     self.view.canvas.get_tk_widget().destroy()
        # self.view.canvas = luck_viz.luck_factor_plot(self.view.frame2, luck_factor_df, week)
        # self.view.canvas.get_tk_widget().grid(row=4, column=0, padx=1, pady=3, rowspan=1, columnspan=1)

    def run(self):
        self.view.init_ui(self)

        visuals = self.model.visuals
        if visuals == []:  # TODO: refactor model response as dictionary with a key:value pair to indicate empty result
            pass
        else:
            self.view.visual_selection.config(values=visuals)
            self.view.visual_selection.set(visuals[0])

            self.view.week_selection.config(values=[i for i in range(18)])  # TODO: Dynamically set values
            self.view.week_selection.set(self.model.eval_week)
            # self.render_graph(None)

        self.view.grid()

    def exit_view(self):
        self.view.grid_forget()


class SeasonPresenter():
    def __init__(self, master, model, view):
        self.master = master
        self.model = model
        self.view = view

    def select_season(self, event=None):
        tree_entry_id = self.view.tree.focus()

        tree_entry_vals = self.view.tree.item(tree_entry_id)['values']
        tree_entry_dict = dict(zip(self.tree_columns, tree_entry_vals))

        self.clear(None)

        self.view.season.insert(0, tree_entry_dict['year'])
        self.view.league_id.insert(0, tree_entry_dict['league_id'])
        self.view.draft_id.insert(0, tree_entry_dict['draft_id'])

    def submit_season(self, event):
        tree_entry_id = self.view.tree.focus()
        tree_entry_vals = self.view.tree.item(tree_entry_id)['values']
        # tree_entry_dict = dict(zip(self.tree_columns, tree_entry_vals))

        year = self.view.season.get()
        league_id = self.view.league_id.get()
        draft_id = self.view.draft_id.get()

        # season_entry = {'year': year,
        #               'league_id': league_id,
        #               'draft_id': draft_id,
        #               }
        self.model.year = year
        self.model.league_id = league_id
        self.model.draft_id = draft_id
        self.model.create()

        self.clear(None)
        self.run()

    def load_season_data(self, event):
        year = self.view.season.get()
        league_id = self.view.league_id.get()
        draft_id = self.view.draft_id.get()

        self.model.year = year
        self.model.league_id = league_id
        self.model.draft_id = draft_id

        self.model.load_season_data()

    def show_tree_menu(self, event):
        # Retrieve selected item from treeview
        item = self.view.tree.focus()

        # If item is selected, display context menu
        if item:
            self.view.tree_menu.post(event.x_root, event.y_root)

    def instantiate_tree(self):
        df = self.model.get_all()
        self.tree_columns = list(df.columns)
        self.view.tree.config(columns=self.tree_columns, selectmode='browse')

        for col in self.tree_columns:
            self.view.tree.column(col, minwidth=0, width=110, stretch=False)
            self.view.tree.heading(col, text=col.title())

    def load_tree_entries(self):
        df = self.model.get_all()
        for item in self.view.tree.get_children():
            self.view.tree.delete(item)

        for row in df.values:
            self.view.tree.insert('', 0, values=list(row))
        # self.view.grid(row=0, column=0, padx=1, pady=2)

    def show_start_view(self, event):
        self.master.change_view(StartView)

    def run(self):
        self.view.init_ui(self)

        self.instantiate_tree()
        self.load_tree_entries()

        self.view.grid()

    def clear(self, event):
        self.view.season.delete(0, 'end')
        self.view.league_id.delete(0, 'end')
        self.view.draft_id.delete(0, 'end')

    def exit_view(self):
        self.view.grid_forget()

