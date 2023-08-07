
import matplotlib.pyplot as plt
import matplotlib

matplotlib.style.use('ggplot')

group1 = list(df3["valeur_gr"])
group2 = list(df3["valeur_mood"])
group3 = list(df3["difference_valeur"])

fig, ax = plt.subplots(figsize=(30,10))

bar_width = 0.2

bar1 = ax.bar(x=range(len(group1)), height=group1, width=bar_width, label='valeur GR', color='purple')
bar2 = ax.bar(x=[i + bar_width for i in range(len(group2))], height=group2, width=bar_width, label='valeur MOOD', color='red')

ax2 = ax.twinx()
bar3 = ax2.bar(x=[i + 2 * bar_width for i in range(len(group3))], height=group3, width=bar_width, label='Difference', color='black')

ax.set_xticks([i + bar_width for i in range(len(group1))])
ax.set_xticklabels(list(df3["partenaire_new"]))

# set y-axis limits for Group 1 and Group 2
ax.set_ylim(0, 3000000000)

# set y-axis step size for Group 1 and Group 2
ax.set_yticks(range(0, 3000000000, 500000000))
ax.ticklabel_format(style='plain', axis='y')

# set y-axis limits for Group 3
ax2.set_ylim(0, 6000000)

# set y-axis step size for Group 3
ax2.set_yticks(range(0, 6000000, 500000))
ax2.ticklabel_format(style='plain', axis='y')


# Add values on the plot (only for Group 3)
x = [i + 2 * bar_width for i in range(len(group3))]
for i, v in enumerate(group3):
    ax2.text(x[i]+bar_width, v-10 ,  str(v*10**(-6))[0:4] + "M", color='black', fontweight='bold', fontsize=20)

ax.legend(loc='upper left')
ax2.legend(loc='upper right')

plt.title("Valeur 2023-02-02", fontsize = "20",fontstyle="italic", fontweight="bold")
plt.savefig("Valeur_2023_02_02_mood_t.png")

