import tkinter as tk
from tkinter import ttk, font


class BaseView(tk.Frame):
    def __init__(self, parent):
        tk.Frame.__init__(self, parent)

    def init_ui(self, presenter):
        ...

    def error_pop_up(self, error_title=None, error_message=None):
        error_window = tk.Toplevel()
        error_window.title = error_title
        error_message_label = tk.Label(error_window, text=error_message)
        error_message_label.pack()


class LoginView(tk.Frame):
    def __init__(self, parent):
        tk.Frame.__init__(self, parent)
        self.parent = parent

    def init_ui(self, presenter):
        # Widget settings
        title_font = font.Font(family='Segoe UI', size=15, weight='bold')
        label_font = font.Font(family='Segoe UI', size=12, weight='bold')
        entry_font = font.Font(family='Segoe UI', size=11, weight='normal')
        button_font = font.Font(family='Segoe UI', size=11, weight='normal')

        # Instantiate widgets
        self.title_text = ttk.Label(self, text='Login', font=title_font)
        # self.login_label = ttk.Label(self, text='Login', font=label_font)
        self.username_entry = ttk.Entry(self, font=entry_font, width=15)
        self.username_entry_text = ttk.Label(self, text='Username:', font=label_font)

        self.data_refresh_var = tk.IntVar()
        self.data_refresh_entry = ttk.Checkbutton(self, text='Refresh Data', variable=self.data_refresh_var)
        self.data_refresh_entry.state(['!selected'])
        # self.data_refresh_entry_text = ttk.Label(self, text='Refresh Data:', font=label_font)
        self.login_button = ttk.Button(self, text='Login')
        self.create_account_button = ttk.Button(self, text='Create Account')

        # Place widgets
        self.title_text.grid(row=0, column=0, pady=15, columnspan=2, sticky='N')
        # self.login_label.grid(row=0, column=1)
        self.username_entry.grid(row=1, column=1, sticky='W', padx=2, pady=1)
        self.username_entry_text.grid(row=1, column=0, sticky='E', padx=2, pady=1)

        self.data_refresh_entry.grid(row=2, column=0, sticky='W', padx=2, pady=1)
        # self.data_refresh_entry_text.grid(row=2, column=1, sticky='W', padx=2, pady=1)

        self.create_account_button.grid(row=3, column=0, sticky='E', padx=2, pady=5)
        self.login_button.grid(row=3, column=1, sticky='W', padx=2, pady=5)

        # Presenter Bindings
        self.login_button.bind('<Button-1>', presenter.login)
        self.create_account_button.bind('<Button-1>', presenter.show_create_account_view)


class CreateAccountView(tk.Frame):
    def __init__(self, parent):
        tk.Frame.__init__(self, parent)
        self.parent = parent

    def init_ui(self, presenter):
        # Widget settings
        title_font = font.Font(family='Segoe UI', size=15, weight='bold')
        label_font = font.Font(family='Segoe UI', size=12, weight='bold')
        entry_font = font.Font(family='Segoe UI', size=11, weight='normal')
        button_font = font.Font(family='Segoe UI', size=11, weight='normal')

        # Instantiate widgets
        self.title_text = ttk.Label(self, text='Create Account', font=title_font)
        # self.login_label = ttk.Label(self, text='Create New Login', font=label_font)
        self.username_entry = ttk.Entry(self, font=entry_font, width=20)
        self.username_entry_text = ttk.Label(self, text='Username:', font=label_font)

        # self.league_id_entry = ttk.Entry(self, font=entry_font, width=20)
        # self.league_id_entry_text = ttk.Label(self, text='League ID:', font=label_font)

        self.login_button = ttk.Button(self, text='Back to Login')
        self.create_account_button = ttk.Button(self, text='Create Account')

        # Place widgets
        self.title_text.grid(row=0, column=0, pady=15, columnspan=2, sticky='N')
        # self.login_label.grid(row=1, column=1)
        self.username_entry.grid(row=1, column=1, sticky='W', padx=2, pady=1)
        self.username_entry_text.grid(row=1, column=0, sticky='E', padx=2, pady=1)

        # self.league_id_entry.grid(row=2, column=1, sticky='W', padx=2, pady=1)
        # self.league_id_entry_text.grid(row=2, column=0, sticky='E', padx=2, pady=1)

        self.login_button.grid(row=3, column=0, sticky='E', padx=2, pady=5)
        self.create_account_button.grid(row=3, column=1, sticky='W', padx=2, pady=5)

        # Presenter Bindings
        self.login_button.bind('<Button-1>', presenter.show_login_view)
        self.create_account_button.bind('<Button-1>', presenter.create_account)


class StartView(tk.Frame):
    """The start/Home page of the application window.

    Attributes
    ----------
    dashboard_selection: str
        the dashboard to display
    """

    def __init__(self, parent):
        tk.Frame.__init__(self, parent)
        self.parent = parent

    def init_ui(self, presenter):
        color1 = '#f7dcb5'
        color2 = '#f7e4b5'

        # Frame 1
        self.frame1 = tk.Frame(self, bd=2, relief='ridge', height=200, width=200, bg=color1)

        # Widget settings
        title_font = font.Font(family='Segoe UI', size=15, weight='bold')
        label_font = font.Font(family='Segoe UI', size=12, weight='bold')
        entry_font = font.Font(family='Segoe UI', size=11, weight='normal')
        button_font = font.Font(family='Segoe UI', size=11, weight='normal')
        action_label_font = font.Font(family='Helvetica', size=18, weight='bold')
        my_font = font.Font(family='Helvetica', size=15)

        button_height = 2
        button_width = 20

        label_height = 2
        # label_width = 20

        # frame1 widgets
        actions_label = tk.Label(self.frame1, text='Import Data', height=label_height, width=button_width, anchor='center',
                                 bg=color1)  # , height=button_height
        button_import_fp_ros_data = tk.Button(self.frame1, text='Import Fantasy Pros ROS Projections', height=button_height,
                                              width=button_width,
                                              bg='SystemButtonFace')
        button_import_fp_week_data = tk.Button(self.frame1, text='Import Fantasy Pros Week Projections',
                                               height=button_height, width=button_width,
                                               bg='SystemButtonFace')

        actions_label.grid(row=0, column=0, padx=1, pady=2)
        button_import_fp_ros_data.grid(row=1, column=0, padx=1, pady=2)
        button_import_fp_week_data.grid(row=2, column=0, padx=1, pady=2)

        # action_label_font.configure(underline=True)
        actions_label['font'] = action_label_font
        button_import_fp_ros_data['font'] = my_font
        button_import_fp_week_data['font'] = my_font

        # frame2
        self.frame2 = tk.Frame(self, bd=2, relief='ridge', height=400, width=400, bg=color2)

        dashboard_header = tk.Label(self.frame2, text='Dashboard', height=label_height, width=60, anchor='center',
                                    bg=color2)
        self.visual_selection = ttk.Combobox(self.frame2, height=button_height, width=20)
        # self.week_selection_text = tk.Label(self.frame2, text='Week:')
        self.week_selection = ttk.Combobox(self.frame2, height=button_height, width=20)
        self.visualization = tk.Label(self.frame2)

        exercise_selection_text = tk.Label(self.frame2, font=label_font)
        week_selection_text = tk.Label(self.frame2, text='week', font=label_font)

        create_visual_button = tk.Button(self.frame2, text='Generate Graphic',
                                               height=button_height, width=button_width,
                                               bg='SystemButtonFace')

        dashboard_header.grid(row=0, column=0, columnspan=1)
        # exercise_selection_text.grid(row=1, column=0, columnspan=1)
        self.visual_selection.grid(row=1, column=0, columnspan=1)
        self.week_selection.grid(row=2, column=0, columnspan=1)
        week_selection_text.grid(row=3, column=0, columnspan=1)
        create_visual_button.grid(row=4, column=0, columnspan=1)

        dashboard_header['font'] = action_label_font

        # Frame placement
        self.frame1.grid(row=0, column=0, rowspan=2, sticky="NSEW")
        self.frame2.grid(row=0, column=1, rowspan=2, sticky="NSEW")

        # Presenter Bindings
        # button_log_workout.bind('<Button-1>', presenter.show_log_workout_view)
        # button_log_exercise.bind('<Button-1>', presenter.show_log_exercise_view)
        # button_log_diet.bind('<Button-1>', presenter.show_date_view)
        # self.visual_selection.bind('<<ComboboxSelected>>', presenter.get_repetitions)
        # self.week_selection.bind('<<ComboboxSelected>>', presenter.render_graph)
        create_visual_button.bind('<Button-1>', presenter.render_graph)


class SeasonView(tk.Frame):
    """The window where a user may enter a workout into the database.

    """

    def __init__(self, parent):
        tk.Frame.__init__(self, parent)
        self.parent = parent

    def init_ui(self, presenter):
        # Widget settings
        title_font = font.Font(family='Segoe UI', size=18, weight='bold')
        header_font = font.Font(family='Segoe UI', size=15, weight='bold')
        label_font = font.Font(family='Segoe UI', size=12, weight='bold')
        entry_font = font.Font(family='Segoe UI', size=11, weight='normal')
        button_font = font.Font(family='Segoe UI', size=11, weight='normal')

        # Frame1
        self.frame1 = tk.Frame(self)
        self.title_text = ttk.Label(self.frame1, text='Season Manager', font=title_font, anchor='center')
        self.tree = ttk.Treeview(self.frame1, show="headings")
        self.title_text.grid(row=0, column=0, sticky="NSEW")
        self.tree.grid(row=1, column=0, sticky="NSEW")

        self.tree_menu = tk.Menu(self.frame1, tearoff=0)
        self.tree_menu.add_command(label="Select", command=lambda: presenter.select_season())

        # Frame1 Presenter Bindings
        # self.tree.bind('<Button-1>', presenter.show_tree_menu)
        self.tree.bind('<Double-Button-1>', presenter.select_season)
        # Frame2
        self.frame2 = tk.Frame(self)

        # Create Widgets
        self.frame2_header_text = ttk.Label(self.frame2, text='Season', font=header_font)
        self.season = ttk.Entry(self.frame2, text='', font=entry_font)
        self.league_id = ttk.Entry(self.frame2, text='', font=entry_font)
        self.draft_id = ttk.Entry(self.frame2, text='', font=entry_font)

        self.season_text = ttk.Label(self.frame2, text='Year: ', font=entry_font)
        self.league_id_text = ttk.Label(self.frame2, text='League ID:', font=entry_font)
        self.draft_id_text = ttk.Label(self.frame2, text='Draft ID: ', font=entry_font)

        # Place entry and text widgets
        self.frame2_header_text.grid(row=0, column=2, pady=15, columnspan=2, sticky='W')

        self.season_text.grid(row=1, column=2, sticky='E', padx=2, pady=1)
        self.league_id_text.grid(row=2, column=2, sticky='E', padx=2, pady=1)
        self.draft_id_text.grid(row=3, column=2, sticky='E', padx=2, pady=1)

        self.season.grid(row=1, column=3, sticky='E', padx=2, pady=1)
        self.league_id.grid(row=2, column=3, sticky='E', padx=2, pady=1)
        self.draft_id.grid(row=3, column=3, sticky='E', padx=2, pady=1)

        # Frame3
        self.frame3 = tk.Frame(self)
        self.button_submit = ttk.Button(self.frame3, text='Create')
        self.button_load = ttk.Button(self.frame3, text='Load Data')
        self.button_exit = ttk.Button(self.frame3, text='Exit')
        self.button_clear = ttk.Button(self.frame3, text='Clear')

        self.button_exit.grid(row=0, column=0, sticky='S', padx=2, pady=5)
        self.button_clear.grid(row=0, column=1, sticky='S', padx=2, pady=5)
        self.button_submit.grid(row=0, column=2, sticky='S', padx=2, pady=5)
        self.button_load.grid(row=0, column=3, sticky='S', padx=2, pady=5)

        self.button_submit.bind('<Button-1>', presenter.submit_season)
        self.button_exit.bind('<Button-1>', presenter.show_start_view)
        self.button_clear.bind('<Button-1>', presenter.clear)
        self.button_load.bind('<Button-1>', presenter.load_season_data)

        # Frame placements
        self.frame1.grid(row=0, column=0, padx=10, pady=2)
        self.frame2.grid(row=1, column=0, padx=1, pady=5, sticky="NSEW")
        self.frame3.grid(row=2, column=0, padx=1, pady=5, sticky="NSEW")