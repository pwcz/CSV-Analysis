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
            var data_summary = []
            for (const obj of json_keys)
            {
                if (obj != "not_matched")
                {
                    let amount =  res.data[obj]["total_amount"];
                    data_summary.push({"description": obj, "amount": this.formatCurrency(amount)});
                }
            }
            this.desserts = data_summary;
        },
        formatCurrency(value)
        {
            return (value.toFixed(2) + " PLN").replace(".", ",");
        },
        onChange(event)
        {
            var data_summary = [];
            if (event.target.value == "not_matched")
            {
                var transactions = this.request_data[event.target.value]
            }
            else
            {
                var transactions = this.request_data[event.target.value].transactions;
            }
            for (const obj of transactions)
            {
                    data_summary.push({"description": obj.description, "amount": this.formatCurrency(obj.amount),
                        "date": obj.date});
            }
            this.details_data = data_summary;

        }
    }
})