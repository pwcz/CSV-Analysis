new Vue({
    el: '#app',
    vuetify: new Vuetify(),
    data: function(){
        return {
            status_message: "hello, please upload file",
            headers: [
                {
                  text: 'Category',
                  align: 'start',
                  sortable: false,
                  value: 'description',
                },
                {
                    text: 'Amount',
                    align: 'end',
                    value: 'amount',
                },
                ],
            desserts: [],
            options: [],
            selected: "default",
            details_header: [
                {
                  text: 'Category',
                  align: 'start',
                  sortable: false,
                  value: 'description',
                },
                {
                    text: 'Amount',
                    align: 'end',
                    value: 'amount',
                },
                {
                    text: 'Date',
                    value: 'date',
                },
                ],
            details_data: [],
            request_data: [],
            excel_data: [],
            spreadsheet_year: 0,
            spreadsheet_month: 0,
            button_disabled: true,
            button_class: 'submit_button',
    }
    },
    methods: {
        async dataSelect()
        {
            this.data_start = this.$refs.data_start.value.split("-");
            const year = this.data_start[0];
            const month = this.data_start[1];

            const headers = { 'Content-Type': 'application/json', 'accept': 'application/json' };
            const res = await axios.get(`/summary/${year}/${month}`, {}, { headers });

            const json_keys = Object.keys(res.data);
            this.options = json_keys;
            this.selected = json_keys[0];
            this.request_data = res.data;
            var data_summary = [];
            var exel_temp = [month];
            for (const obj of json_keys)
            {
                if (obj != "not_matched")
                {
                    let amount =  res.data[obj]["total_amount"];
                    data_summary.push({"description": obj, "amount": this.formatCurrency(amount)});
                    exel_temp.push(amount.toFixed(2));
                }
            }
            this.desserts = data_summary;
            this.excel_data = exel_temp;
            this.spreadsheet_year = year;
            this.spreadsheet_month = month;

            this.button_disabled = false;
            this.button_class = 'submit_button enabled';
            this.details_data = this.update_data_summary(this.request_data[this.options[0]].transactions);
        },
        formatCurrency(value)
        {
            return (value.toFixed(2) + " PLN").replace(".", ",");
        },
        onChange(event)
        {

            if (event.target.value == "not_matched")
            {
                var transactions = this.request_data[event.target.value]
            }
            else
            {
                var transactions = this.request_data[event.target.value].transactions;
            }

            this.details_data = this.update_data_summary(transactions);

        },
        update_data_summary(transactions)
        {
            var data_summary = [];
            for (const obj of transactions)
            {
                    data_summary.push({
                            "description": obj.description,
                            "amount": this.formatCurrency(obj.amount),
                            "date": obj.date
                        });
            }
            return data_summary;
        },
        async submit_excel(event)
        {
            if(this.excel_data.length != 0)
            {
                const headers = { 'Content-Type': 'application/json', 'accept': 'application/json' };
                const spreadsheet_data = {"summary": this.excel_data}
                const res = await axios.post(`/summary/${this.spreadsheet_year}/${this.spreadsheet_month}`, spreadsheet_data, { headers });
                console.log(res);
                if (res.status == 200)
                {
                    alert("Spread sheet updated!");
                }
                else
                {
                    alert("Upload error: " + res.statusText);
                }
            }
            else
            {
                console.log("Empty excel data");
            }
        },
    }
})