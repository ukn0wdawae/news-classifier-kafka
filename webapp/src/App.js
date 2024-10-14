import React, {useState} from "react";
import axios from "axios";

function App() {
  const [searchItem, SetSearchItem] = useState('')
  const handleChange = (event) => {
    SetSearchItem( {"searchText" : event.target.value})
    }
  const [category, SetCategory] = useState('')
  const handleSubmit= async(e)=>{
    e.preventDefault()
    let req = {
        method: 'post',
        contentType: 'application/json',
        url: "http://0.0.0.0:5500/predict",
        data: searchItem
      }
      await axios(req).then(res => {
        if(res.data.category){
            alert("req Sent!")
        }
      }, (error) => console.log(error));
}
  return (
    <div >
      <nav className="black white-text">
        <div className="nav-wrapper">
          <a href="#" className="brand-logo center">Big Data News Classification using Kafka</a>
        </div>
      </nav>
      <div className="container">
        <br/>
        <br/>
        <br/>
        <div className="row card-panel">
        <form action="#" className="" method="post" onSubmit={handleSubmit}>
                <div className="input-field">
                    <h6>Search Text: </h6>
                    <input type="text" name="Uname" id="Uname" placeholder='Enter Text' required autocomplete="off" style={{"width" :"100%" }} onChange={handleChange}/>
                    <label ></label>
                </div>
                <div className="input-field">
                    <button type="submit" className="btn" value="Submit"> Submit</button>
                </div>  
            </form>
            <h6>Result: </h6>
            <div id="result_message">
              {category}
            </div>

    </div>
        </div>

    </div>
  );
}

export default App;
