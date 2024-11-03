from app.view import *
from app.model import *
from app.presenter import *



class AppController(tk.Tk):
    '''
    TODO: Menu bar will appear on the Login menu when it should not.
    '''
    def __init__(self):
        super().__init__()

        # TODO: move root and root frame outside of the controller
        self.container = tk.Frame(self)

        self.title('AthleteDB')

        self.container.grid(row=0, column=0, padx=2, pady=2, ipadx=0, ipady=0, sticky='N')

        self.api

        self.views = {LoginView: {'model': AccountModel, 'presenter': LoginPresenter},
                      CreateAccountView: {'model': AccountModel, 'presenter': CreateAccountPresenter},
                      StartView: {'model': StartModel, 'presenter': StartPresenter},
                      SeasonView: {'model': SeasonModel, 'presenter': SeasonPresenter}
                      }

        self.active_presenter = None
        self.active_view = None
        self.active_model = None
        self.account_model = None

        self.change_view(LoginView)

    def change_view(self, view):
        presenter = self.views[view]['presenter']
        model = self.views[view]['model']

        if self.active_presenter is not None:
            self.active_presenter.exit_view()
            # self.active_view.destroy()
            # self.active_model.destroy()
            # self.active_presenter.destroy() # This should drop the reference count for model, view, and presenter to 0

        # instantiate model
        if presenter == LoginPresenter:
            self.config(menu='')
            if self.account_model != None:
                self.close_connection()
            self.account_model = model()
            self.active_model = self.account_model
        elif presenter == CreateAccountPresenter:
            self.account_model = model()
            self.active_model = model()
        else:
            self.add_menu()
            self.active_model = model(self.account_model.api)

        self.geometry('') # Reset root window geometry

        # Instantiate view and presenter
        self.active_view = view(self.container)
        self.active_presenter = presenter(self, self.active_model, self.active_view)

        self.active_presenter.run()

    def close_connection(self):
        try:
            self.account_model.db_conn.close()
        except:
            print('There was an error closing the database connection. It\'s possible that it never existed in the ',
                  'first place.')

    def add_menu(self):
        """Define the menubar configuration."""

        menubar = tk.Menu(self)
        self.config(menu=menubar)

        # File menu
        filemenu = tk.Menu(menubar, tearoff=0)
        filemenu.add_command(label="Home", command=lambda: self.change_view(StartView))
        filemenu.add_separator()
        filemenu.add_command(label="Login", command=lambda: self.change_view(LoginView))

        # Data Menu
        datamenu = tk.Menu(menubar, tearoff=0)
        datamenu.add_command(label="Seasons", command=lambda: self.change_view(SeasonView))
        # datamenu.add_command(label="Create Exercise", command=lambda: self.change_view(ExerciseView))
        # datamenu.add_command(label="Create Goal", command=lambda: self.change_view(GoalView))
        # datamenu.add_command(label="Mass Upload")
        datamenu.add_separator()
        # datamenu.add_command(label="View Data", command=lambda: self.change_view(TableDataView))
        # datamenu.add_command(label="Glossary", command=lambda: self.change_view(GlossaryView))

        # Visuals Menu
        visualsmenu = tk.Menu(menubar, tearoff=0)
        # visualsmenu.add_command(label="")
        # visualsmenu.add_command(label="")

        # Info Menu
        infomenu = tk.Menu(menubar, tearoff=0)
        # infomenu.add_command(label="Program Info", command=lambda: self.change_view(ProgramInfoView))
        # infomenu.add_command(label="FAQ", command=lambda: self.change_view(FAQView))

        menubar.add_cascade(label="File", menu=filemenu)
        menubar.add_cascade(label="Data", menu=datamenu)
        menubar.add_cascade(label="Visuals", menu=visualsmenu)
        menubar.add_cascade(label="Info", menu=infomenu)
