import React, { useContext, useEffect } from "react";
import { Route, Switch, useRouteMatch } from "react-router-dom";

import { AppHome } from "./Home";
import Login from "../pages/citizen/Login";

const getTenants = (codes, tenants) => {
  return tenants.filter((tenant) => codes.map((item) => item.code).includes(tenant.code));
};

export const AppModules = ({ stateCode, userType, modules, appTenants }) => {
  const ComponentProvider = Digit.Contexts.ComponentProvider;
  const { path } = useRouteMatch();
  const registry = useContext(ComponentProvider);

  useEffect(() => {
    if (userType !== "citizen") {
      const user = Digit.SessionStorage.get("Employee.user-details");
      Digit.UserService.setUser(user);
    }

    return () => {
      if (userType !== "citizen") {
        Digit.UserService.setUser({});
      }
    };
  }, []);

  const appRoutes = modules.map(({ code, tenants }, index) => {
    const Module = registry.getComponent(`${code}Module`);
    return (
      <Route key={index} path={`${path}/${code.toLowerCase()}`}>
        <Module stateCode={stateCode} cityCode="pb.amritsar" moduleCode={code} userType={userType} tenants={getTenants(tenants, appTenants)} />
      </Route>
    );
  });

  return (
    <Switch>
      {appRoutes}
      {userType === "citizen" && (
        <Route path={`${path}/login`}>
          <Login stateCode={stateCode} cityCode="pb.amritsar" />
        </Route>
      )}
      <Route>
        <AppHome userType={userType} modules={modules} />
      </Route>
    </Switch>
  );
};
