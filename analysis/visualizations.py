import pandas as pd
import numpy as np
from pipeline.api import FantasyApi
from matplotlib import pyplot as plt
from matplotlib.patches import Circle
import matplotlib.colors as mcolors
# import matplotlib
import imageio, PIL


def autolabel(ax, rects, luck_factor_df, xpos='center'):
    """
    Attach a text label above each bar in *rects*, displaying its height.

    *xpos* indicates which side to place the text w.r.t. the center of
    the bar. It can be one of the following {'center', 'right', 'left'}.
    """
    plt.rcParams.update({'font.size': 10, 'font.weight': 'normal', 'font.family': 'DejaVu Sans'})
    ha = {'center': 'center', 'right': 'left', 'left': 'right'}
    offset = {'center': 0, 'right': 1, 'left': -1}

    for i, rect in enumerate(rects):
        height = rect.get_height()
        luck_factor = round(luck_factor_df.iloc[i]['luck_factor'], 1)
        ax.annotate('{}'.format(round(height, 1)),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(offset[xpos] * 3, 3),  # use 3 points offset
                    textcoords="offset points",  # in both directions
                    ha=ha[xpos], va='bottom')

def luck_factor_plot(luck_factor_df, week):
    # matplotlib.use('TKAgg')
    luck_factor_df = luck_factor_df.sort_values('wins', ascending=False)
    img_file = r'images\lucky.jpg'
    background_image = imageio.v2.imread(img_file, pilmode='RGBA')

    fig, ax = plt.subplots(1, 1)
    fig.patch.set_facecolor(mcolors.CSS4_COLORS['mediumseagreen'])
    fig.patch.set_alpha(0.4)

    plt.rcParams.update({'font.size': 10, 'font.weight': 'bold', 'font.family': 'DejaVu Sans'})

    ind = np.arange(12)  # x locations of the rosters
    width = .35  # The width of the bars

    rects1 = ax.bar(ind - width / 2, luck_factor_df['wins'], width, label='Wins', color=mcolors.CSS4_COLORS['gold'],
                    edgecolor=mcolors.CSS4_COLORS['dimgrey'])
    rects2 = ax.bar(ind + width / 2, luck_factor_df['true_wins'], width, label='Expected Wins',
                    color=mcolors.CSS4_COLORS['lightgrey'], edgecolor=mcolors.CSS4_COLORS['black'])

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ylim = [0, max(luck_factor_df['wins'].max() * 1.5, 5)]
    xlim = [0, ax.get_xlim()[1]]
    ax.set_ylabel('Wins', fontdict={'fontsize': 13, 'fontweight': 'bold'})
    ax.set_title(f'Week {week} Recap: Schedule Luck by Roster', fontdict={'fontsize': 14, 'fontweight': 'bold'})
    ax.set_xticks(ind)
    ax.set_xticklabels(luck_factor_df['display_name'], fontdict={'fontsize': 11, 'fontweight': 'bold'})
    plt.xticks(rotation=40, ha='right')
    ax.set_ylim([ylim[0], ylim[1]])
    ax.legend()
    ax.text(xlim[1] / 2 - 4, ylim[1] * 4 / 5 + .65, 'Schedule Wins (Expected Wins - Actual Wins)')

    autolabel(ax, rects1, luck_factor_df)
    autolabel(ax, rects2, luck_factor_df)

    plt.rcParams.update({'font.size': 10, 'font.weight': 'bold', 'font.family': 'DejaVu Sans'})
    # Luck Index and circle plots
    max_luck = luck_factor_df['luck_factor'].max()
    for i, rect in enumerate(rects1):
        luck_factor = round(luck_factor_df.iloc[i]['luck_factor'], 1)
        height = rect.get_height()
        circle_color = (0.45 - (luck_factor / max_luck) * .20, 0.45 + (luck_factor / max_luck) * .20, 0.1, 0.3)
        circ = Circle((rect.get_x() + rect.get_width(), ylim[1] * 4 / 5 + .2), radius=.35, fill=True, edgecolor='k',
                      facecolor=circle_color, linewidth=1.2)
        plt.gca().add_patch(circ)
        ax.annotate('{}'.format(luck_factor),
                    xy=(rect.get_x() + rect.get_width(), ylim[1] * 4 / 5 + .2),
                    xytext=(0, -5),  # use 0 points offset
                    textcoords="offset points",  # in both directions
                    ha='center', va='bottom')

    fig.tight_layout()
    ax.imshow(background_image, extent=[ax.get_xlim()[0], xlim[1], ylim[0], ylim[1]])
    plt.savefig(
        f'Schedule Luck - Week {week}.jpeg',
        facecolor=mcolors.CSS4_COLORS['lightseagreen'])
    plt.show()

def roster_week_points_plot(roster_week_points_df, week):
    roster_week_points_df = roster_week_points_df.sort_values('roster_id', ascending=True)
    img_file = r'images\crown.png'
    background_image = imageio.v2.imread(img_file, pilmode='RGBA')

    fig, ax = plt.subplots(1, 1)
    fig.patch.set_facecolor(mcolors.CSS4_COLORS['seagreen'])
    fig.patch.set_alpha(0.5)
    ax.set_facecolor(mcolors.CSS4_COLORS['lightgrey'])
    ax.set_alpha(0.2)

    plt.rcParams.update({'font.size': 10, 'font.weight': 'bold', 'font.family': 'DejaVu Sans'})

    # cmap = cm.get_cmap(''Diverging'')
    xtick_spacing = 20
    ind = np.arange(1, 13)*xtick_spacing  # x locations of the rosters
    # width = .35  # The width of the bars
    ax.scatter(roster_week_points_df['roster_id']*xtick_spacing, roster_week_points_df['points'], c=roster_week_points_df['points'], cmap='RdYlGn', vmin=60, vmax=140, marker='o', edgecolor='k', linewidth=.3, s=100)

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ylim = [40, roster_week_points_df['points'].max() * 1.05]
    xlim = [0, ax.get_xlim()[1]]
    ax.set_ylabel('Points', fontdict={'fontsize': 13, 'fontweight': 'bold'})
    ax.set_title(f'Week {week} Recap: Weekly Points by Roster', fontdict={'fontsize': 14, 'fontweight': 'bold'})
    ax.set_xticks(ind)
    ax.set_xticklabels(roster_week_points_df['display_name'].unique(), fontdict={'fontsize': 11, 'fontweight': 'bold'})
    plt.xticks(rotation=40, ha='right')
    ax.set_ylim([ylim[0], ylim[1]])
    ax.set_xlim([xlim[0], xlim[1]])
    # ax.legend()
    # ax.text(xlim[1] / 2 - 3.5, ylim[1] * 4 / 5 + .65, 'Schedule Wins (Expected Wins - Actual Wins)')

    # autolabel(ax, rects1, luck_factor_df)
    # autolabel(ax, rects2, luck_factor_df)

    plt.rcParams.update({'font.size': 10, 'font.weight': 'bold', 'font.family': 'DejaVu Sans'})
    ax.grid(axis='y')

    # axin = ax.inset_axes([60, 40, 60+2*(120-105), ylim[0]+2*(120-105)], transform=ax.transData)  # create new inset axes in data coordinates
    king_team = 9 #TODO: Dynamically assign
    king_points = roster_week_points_df['points'].max() #TODO: Dynamically assign
    king_team = int(roster_week_points_df[roster_week_points_df['points'] == king_points]['roster_id'].iloc[0])
    # king_points = float(roster_week_points_df['points'].max())
    extent = [king_team*20-2, king_team*20+2, king_points+1, king_points+6]
    ax.imshow(background_image, extent=extent)
    # axin.axis('off')
    fig.tight_layout()
    plt.savefig(
        f'Week {week} Point Dist.png',
        facecolor=mcolors.CSS4_COLORS['lightseagreen'])
    plt.show()
