let app = Vue.createApp({
    data: function(){
        return {
            status_message: "hello, please upload file",
            transaction_type: "",
            new_transactions: 0,
            duplicates: 0,
        }
    },
    methods: {
        uploadFile() {
            this.Images = this.$refs.file.files[0];
            console.log(this.Images)
        },
        async submitFile() {
            console.log('submitFile')
            const formData = new FormData();
            formData.append('in_file', this.Images);
            const headers = { 'Content-Type': 'multipart/form-data', 'accept': 'application/json' };
            const res = await axios.post('http://127.0.0.1:8000/upload_file', formData, { headers });
            console.log(res);
            this.status_message = res.data.status;
            this.new_transactions = res.data.inserted;
            this.duplicates = res.data.duplicates;
            this.transaction_type = res.data.type;
    }
    }
})
app.mount('#app')