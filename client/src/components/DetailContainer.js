import React from "react";

export default function DetailContainer({ selected }) {
  return (
    <div>
      {selected === "matters" && (
        <div>
          <h2>Why it Matters</h2>
          <p>
            Tofu coconut summertime soy milk grenadillo avocado vine tomatoes
            thyme pineapple salsa Thai sun pepper cauliflower soba noodles
            hummus falafel bowl Malaysian blueberry chia seed jam mushroom
            risotto.
          </p>
          <p>
            Lemon black bean chili dip veggie burgers paprika edamame hummus
            matcha. Red amazon pepper smoky maple tempeh glaze jalapeño Italian
            linguine puttanesca orange.
          </p>
        </div>
      )}
      {selected === "methods" && (
        <div>
          <h2>Our Methods</h2>
          <p>
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras in
            risus nisl. Donec semper ligula ac ex consequat laoreet non et
            tortor. Ut cursus, nisl ut suscipit pretium, sapien ex tristique mi,
            a luctus felis quam a mi. Nulla a massa sodales, aliquam lacus
            vitae, lacinia mauris.
          </p>
          <p>
            Nunc eget viverra dolor. Praesent ac diam aliquet, congue nibh sed,
            egestas risus. Vivamus consequat ligula at sem mattis iaculis.
          </p>
          <p>
            In hac habitasse platea dictumst. Nullam at dapibus diam. Phasellus
            sed urna porta, malesuada turpis at, efficitur libero. Nullam eu
            lobortis sapien, quis pharetra sapien. Vivamus semper tristique
            orci, eu rhoncus eros molestie at. Suspendisse maximus aliquet massa
            id faucibus. Pellentesque efficitur vitae mauris non vulputate.
          </p>
        </div>
      )}
      {selected === "findings" && (
        <div>
          <h2>Our Findings</h2>
          <p>
            Indian spiced dragon fruit leek main course figs veggie burgers
            blood orange smash hemp seeds red curry tofu noodles maple orange
            tempeh sweet potato black bean burrito winter dessert springtime
            strawberry thyme Thai chickpea.
          </p>
          <div className="data-vis">
            <div className="graph">
              <div className="graph-bar-red graph-bar"></div>
              <div className="graph-bar-green graph-bar"></div>
              <div className="graph-bar-purple graph-bar"></div>
            </div>
          </div>
          <p>
            Crust pizza fruit smash strawberry spinach salad summertime citrusy
            pasta tahini drizzle edamame hummus lentils falafel bites
            overflowing.
          </p>
        </div>
      )}
      {selected === "cta" && (
        <div className="cta">
          <h2>Call to Action</h2>
          <p>
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras in
            risus nisl. Donec semper ligula ac ex consequat laoreet non et
            tortor. Ut cursus, nisl ut suscipit pretium, sapien ex tristique mi,
            a luctus felis quam a mi. Nulla a massa sodales, aliquam lacus
            vitae, lacinia mauris. Sed non orci nibh. Vivamus eget massa cursus,
            lobortis nibh ac, semper nibh. Sed aliquet massa et vehicula
            fermentum. Nunc sed scelerisque lacus. Quisque condimentum libero eu
            magna aliquam, id eleifend nibh sagittis.
          </p>
          <div className="btn-container">
            <a
              target="_blank"
              rel="noreferrer"
              href="https://www.chicago.gov/city/en/depts/mayor/iframe/lookup_ward_and_alderman.html"
            >
              <button>Find Your Alderperson</button>
            </a>
          </div>
        </div>
      )}
    </div>
  );
}
