import matplotlib.pyplot as plt
import pandas as pd

class Visualisations:
    def __init__(self, data):
        self.month_names = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December",
        }
        self.df = self.dataframe(data)

    def dataframe(self, data):
        df = pd.DataFrame(data)
        if "_id" in df.columns:
            df["month"] = df["_id"] 
            df.drop(columns=["_id"], inplace=True)

        df['month'] = pd.to_numeric(df['month'], downcast='integer', errors='coerce')
        df = df.sort_values("month")
        return df

    def proportion_by_month(self):
        plt.figure(figsize=(8, 8))
        plt.pie(
            self.df["total_cases"],
            labels=self.df["month"],
            autopct="%1.1f%%",
            startangle=140,
            colors=plt.cm.Paired.colors,
        )

        legend_labels = [f"{num} = {name}" for num, name in self.month_names.items() if num in self.df["month"].values]
        plt.legend(legend_labels, title="Months", fontsize=10, loc="upper right",  bbox_to_anchor=(0.06, 0.8))
        plt.title("Proportion of Cases by Month", fontsize=16)
        return plt.show()

    def compare_cases_and_deaths(self):
        plt.figure(figsize=(10, 6))
        plt.bar(self.df["month"], self.df["total_cases"], label="Total Cases", alpha=0.7, color="blue")
        plt.bar(self.df["month"], self.df["total_deaths"], label="Total Deaths", alpha=0.7, color="red", bottom=self.df["total_cases"])
        plt.title("Monthly Total Cases and Deaths", fontsize=16)
        plt.xlabel("Month", fontsize=14)
        plt.ylabel("Count", fontsize=14)
        plt.xticks(self.df["month"])
        plt.legend(fontsize=12)
        plt.grid(axis="y", alpha=0.5)
        plt.show()