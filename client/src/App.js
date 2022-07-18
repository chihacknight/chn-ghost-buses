import "./css/main.css";
import Map from "./components/Map";
import Header from "./components/Header";
import ProjectScope from "./components/ProjectScope";
import Intro from "./components/Intro";
import Footer from "./components/Footer";
import Socials from "./components/Socials";

function App() {
  return (
    <div className="App">
      <div className="container">
        <Socials />
        <Header />
        <Intro />
        <hr />
        <ProjectScope />
        <hr />
        <Map />
      </div>
      <Footer />
    </div>
  );
}

export default App;
